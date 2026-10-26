package scheduler

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// nextActivation carries a computed activation back to the loop's critical
// section.
//
// Fields:
//   - entry (*entry): entry the activation was computed for.
//   - next (time.Time): activation computed by the entry's schedule.
type nextActivation struct {
	entry *entry
	next  time.Time
}

// entryHeap is a min-heap of entries keyed on next activation, guarded by
// [Scheduler.mu].
type entryHeap []*entry

// Len returns the heap size.
//
// Returns:
//   - n (int): number of entries in the heap.
func (h entryHeap) Len() (n int) {
	n = len(h)

	return
}

// Less orders by next activation, then by identifier for determinism.
//
// Parameters:
//   - i (int): index of the first entry.
//   - j (int): index of the second entry.
//
// Returns:
//   - less (bool): true when the entry at i sorts before the entry at j.
func (h entryHeap) Less(i, j int) (less bool) {
	if h[i].next.Equal(h[j].next) {
		less = h[i].id < h[j].id

		return
	}

	less = h[i].next.Before(h[j].next)

	return
}

// Swap swaps two entries and keeps their heap indices in sync.
//
// Parameters:
//   - i (int): index of the first entry.
//   - j (int): index of the second entry.
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

// Push appends an [*entry] to the heap.
//
// Parameters:
//   - x (any): entry to append; must be an [*entry].
func (h *entryHeap) Push(x any) {
	e := x.(*entry)

	e.heapIndex = len(*h)

	*h = append(*h, e)
}

// Pop removes the last entry; [heap.Pop] moves the minimum there first.
//
// Returns:
//   - x (any): removed entry, typed as [*entry].
func (h *entryHeap) Pop() (x any) {
	old := *h

	n := len(old)

	e := old[n-1]
	old[n-1] = nil
	e.heapIndex = -1

	*h = old[:n-1]

	x = e

	return
}

// Scheduler registers [Job] values against schedules and executes them
// concurrently on their activation times.
//
// # Lifecycle
//
//  1. Construct with [NewScheduler], optionally passing [OptionFunc] values.
//  2. Register jobs with [Scheduler.Register] (cron expression, descriptor, or
//     `@every` interval) or [Scheduler.RegisterSchedule] (a pre-built
//     [Schedule], including custom implementations); registration is possible
//     both before and after start.
//  3. Call [Scheduler.Start] to launch the scheduling loop (non-blocking).
//  4. Call [Scheduler.Stop] to shut down gracefully: the loop halts, per-run
//     contexts are cancelled, and in-flight runs are awaited.
//
// A successfully stopped scheduler may be started again: [Scheduler.Start]
// after a [Scheduler.Stop] that completed launches a fresh loop and every entry
// is rescheduled from the current time. A Stop that timed out does not reset the
// entries and leaves the scheduler unusable for a restart; recreate it.
//
// # Run semantics
//
// Each entry computes its next activation with [Schedule.Next]; the loop
// sleeps until the earliest activation. On fire, the job runs in its own
// goroutine (bounded by the [WithMaxConcurrency] semaphore) with a per-run
// context that is cancelled by [Scheduler.Stop]. Overlapping runs of the
// same entry are SKIPPED: if an entry is still in flight when its next
// activation arrives, that activation is dropped and the entry is
// rescheduled.
//
// The zero value is not usable; construct through [NewScheduler]. A Scheduler must
// not be copied after first use.
//
// Fields:
//   - mu (sync.Mutex): guards every mutable field below.
//   - location (*time.Location): location cron expressions are evaluated in.
//   - clock (func() time.Time): time source (test seam).
//   - sem (chan struct{}): concurrency semaphore; nil when unlimited.
//   - onError (OnError): failure callback.
//   - entries (map[int]*entry): registered entries by identifier.
//   - queue (entryHeap): entries with a computed future activation.
//   - pending ([]*entry): entries awaiting their first evaluation.
//   - nextID (int): next entry identifier to hand out.
//   - generation (int): identifies the active scheduling loop.
//   - started (bool): whether the scheduling loop is running.
//   - runCancel (context.CancelFunc): cancels the active generation's run context.
//   - loopDone (chan struct{}): closed when the active loop exits.
//   - wake (chan struct{}): signals the loop to recompute its sleep.
//   - wg (sync.WaitGroup): tracks in-flight runs for Stop.
type Scheduler struct {
	mu         sync.Mutex
	location   *time.Location
	clock      func() time.Time
	sem        chan struct{}
	onError    OnError
	entries    map[int]*entry
	queue      entryHeap
	pending    []*entry
	nextID     int
	generation int
	started    bool
	runCancel  context.CancelFunc
	loopDone   chan struct{}
	wake       chan struct{}
	wg         sync.WaitGroup
}

// Register registers job to run on the schedule parsed from spec (see [Parse]).
// It is a convenience wrapper: it parses spec and delegates to
// [Scheduler.RegisterSchedule].
//
// Parameters:
//   - spec (string): the cron expression, descriptor, or `@every` interval.
//   - job (Job): the unit of work to register; must not be nil.
//
// Returns:
//   - entryID (int): the identifier of the new entry, for [Scheduler.Unregister].
//   - err (error): non-nil when spec fails to parse or job is nil.
func (s *Scheduler) Register(spec string, job Job) (entryID int, err error) {
	schedule, err := Parse(spec)
	if err != nil {
		err = fmt.Errorf("scheduler.Register: parsing %q: %w", spec, err)

		return
	}

	entryID, err = s.RegisterSchedule(schedule, job)

	return
}

// RegisterSchedule registers job to run on schedule and wakes the scheduling
// loop. Unlike [Scheduler.Register] it accepts a ready-made [Schedule] —
// built with [Parse], one of the package's concrete types, or a custom
// implementation — instead of parsing a specification string.
//
// The entry is queued for its first schedule evaluation rather than evaluated
// here: [Schedule.Next] is user code and never runs while holding the
// scheduler's internal mutex.
//
// Parameters:
//   - schedule (Schedule): the schedule the job fires on; must not be nil.
//   - job (Job): the unit of work to register; must not be nil.
//
// Returns:
//   - entryID (int): the identifier of the new entry, for [Scheduler.Unregister].
//   - err (error): non-nil when schedule or job is nil.
func (s *Scheduler) RegisterSchedule(schedule Schedule, job Job) (entryID int, err error) {
	if job == nil {
		err = errors.New("scheduler.RegisterSchedule: job is nil")

		return
	}

	if schedule == nil {
		err = errors.New("scheduler.RegisterSchedule: schedule is nil")

		return
	}

	s.mu.Lock()

	s.nextID++

	entryID = s.nextID

	e := &entry{
		id:        entryID,
		job:       job,
		schedule:  schedule,
		heapIndex: -1,
		queued:    true,
	}

	s.entries[entryID] = e
	s.pending = append(s.pending, e)

	s.mu.Unlock()

	s.signal()

	return
}

// Unregister unregisters the entry with the given identifier. Unregistering
// an unknown identifier is a no-op. An in-flight run of the unregistered
// entry is not interrupted.
//
// Parameters:
//   - entryID (int): the identifier returned by [Scheduler.Register].
func (s *Scheduler) Unregister(entryID int) {
	s.mu.Lock()

	e, existed := s.entries[entryID]

	if existed {
		delete(s.entries, entryID)

		if e.heapIndex >= 0 {
			heap.Remove(&s.queue, e.heapIndex)
		}
	}

	s.mu.Unlock()

	s.signal()
}

// Entries returns a snapshot copy of every registered entry, ordered by
// entry identifier. Mutating the returned values never affects the
// scheduler's internal state.
//
// Returns:
//   - entries ([]Entry): the registered entries, ordered by identifier.
func (s *Scheduler) Entries() (entries []Entry) {
	s.mu.Lock()

	entries = make([]Entry, 0, len(s.entries))

	for _, e := range s.entries {
		entries = append(entries, e.snapshot())
	}

	s.mu.Unlock()

	// Sort after releasing the lock: the comparator is cheap but the loop
	// holds the same mutex, so the critical section stays minimal.
	slices.SortFunc(entries, func(a, b Entry) int { return cmp.Compare(a.ID, b.ID) })

	return
}

// Start launches the scheduling loop in its own goroutine and returns
// immediately. It is a no-op if the scheduler is already started; after a
// successful Stop, or after the lifecycle context has been cancelled, it
// starts a fresh loop. The context parents every per-run context and cancels
// them when cancelled; unlike Stop, it does not wait for those runs or reset
// the entries. Start and Stop must be serialized. See [Scheduler] for
// lifecycle and run semantics.
//
// Parameters:
//   - ctx (context.Context): the lifecycle context of the scheduler.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()

	if s.started {
		s.mu.Unlock()

		return
	}

	s.started = true
	s.generation++

	runCtx, runCancel := context.WithCancel(ctx)

	s.runCancel = runCancel
	s.loopDone = make(chan struct{})

	generation := s.generation
	loopDone := s.loopDone

	s.mu.Unlock()

	go s.loop(runCtx, generation, loopDone)
}

// Stop gracefully shuts the scheduler down: it halts the loop, cancels every
// per-run context, and waits for in-flight runs until ctx expires. It is a
// no-op on an already-stopped scheduler. Start and Stop must be serialized.
// If the wait times out, the scheduler is left stopped with entries not reset
// and later Stop calls return immediately; do not restart it — recreate the
// Scheduler once those runs finish. See [Scheduler] for lifecycle and run
// semantics.
//
// Parameters:
//   - ctx (context.Context): bounds how long Stop waits for in-flight runs.
//
// Returns:
//   - err (error): non-nil when ctx expired before shutdown completed.
func (s *Scheduler) Stop(ctx context.Context) (err error) {
	s.mu.Lock()

	if !s.started {
		s.mu.Unlock()

		return
	}

	s.started = false

	s.runCancel()

	loopDone := s.loopDone

	s.mu.Unlock()

	select {
	case <-loopDone:
	case <-ctx.Done():
		err = fmt.Errorf("scheduler.Stop: timed out waiting for the scheduling loop: %w", context.Cause(ctx))

		return
	}

	// A job run may call [Scheduler.Stop] itself, so waiting for in-flight
	// runs inline would deadlock: wait on a side goroutine and abandon the
	// wait when ctx expires first.
	waited := make(chan struct{})

	go func() {
		s.wg.Wait()

		close(waited)
	}()

	select {
	case <-waited:
	case <-ctx.Done():
		err = fmt.Errorf("scheduler.Stop: timed out waiting for in-flight runs: %w", context.Cause(ctx))

		return
	}

	s.mu.Lock()

	// Reset every entry to its unevaluated state so a later [Scheduler.Start]
	// reschedules all of them from the current time.
	for _, e := range s.entries {
		e.running = false
		e.next = time.Time{}
		e.heapIndex = -1

		if !e.queued {
			e.queued = true

			s.pending = append(s.pending, e)
		}
	}

	// Drop the pointers the queue's backing array still holds before
	// truncating, so entries unregistered after the stop are not retained.
	clear(s.queue)

	s.queue = s.queue[:0]

	s.mu.Unlock()

	return
}

// signal wakes the scheduling loop so it recomputes its sleep. It never
// blocks: a pending wake-up is coalesced.
func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// now returns the current time from the configured clock, expressed in the
// configured location.
//
// Returns:
//   - now (time.Time): the scheduler's current time.
func (s *Scheduler) now() (now time.Time) {
	now = s.clock().In(s.location)

	return
}

// loop is the scheduling loop: it fires every due entry, recomputes the
// affected schedules, then sleeps until the earliest scheduled activation, a
// wake-up signal, or shutdown.
//
// Scheduled entries live in a min-heap keyed on their next activation, so an
// iteration touches only the entries that are actually due instead of
// scanning every registered entry. [Schedule.Next] is user-supplied code and
// is NEVER invoked while holding s.mu — a schedule calling back into the
// scheduler must not deadlock it: due entries are popped and unevaluated
// entries collected under the lock, their next activations are computed
// after the lock is released, and the results are applied under a second
// critical section that skips entries removed in the meantime and applies
// nothing when the loop was superseded (the stop that superseded it already
// re-queued every entry). An entry whose schedule reports no future
// activation stays out of the heap until the next [Scheduler.Stop]–
// [Scheduler.Start] cycle re-evaluates every entry.
//
// The generation identifies the [Scheduler.Start] call the loop belongs to:
// when a restart supersedes it, the loop exits instead of double-firing
// jobs. loopDone is closed on exit, runCtx parents every per-run context.
//
// Parameters:
//   - runCtx (context.Context): the run context of this generation.
//   - generation (int): the generation this loop belongs to.
//   - loopDone (chan struct{}): closed when the loop exits.
func (s *Scheduler) loop(runCtx context.Context, generation int, loopDone chan struct{}) {
	defer close(loopDone)
	defer func() {
		s.mu.Lock()

		// Only the generation that owns this loop may clear the flag; a
		// concurrent Start may already have installed a newer loop.
		if s.generation == generation {
			s.started = false
		}

		s.mu.Unlock()
	}()

	for {
		// Sample the clock before taking the mutex: s.now calls the
		// user-supplied clock, which must never run under s.mu.
		now := s.now()

		s.mu.Lock()

		if !s.started || s.generation != generation || runCtx.Err() != nil {
			s.mu.Unlock()

			return
		}

		// Pop every due entry off the heap; a popped entry stays out of the
		// heap until its next activation is recomputed and applied below.
		var due []*entry

		for len(s.queue) > 0 && !s.queue[0].next.After(now) {
			e := heap.Pop(&s.queue).(*entry)

			due = append(due, e)
		}

		// Fire each due entry in its own goroutine, dropping the activation
		// when the previous run is still in flight. Fired or dropped, the
		// entry is rescheduled from now, outside the lock.
		reschedule := make([]*entry, 0, len(due)+len(s.pending))

		for _, e := range due {
			if !e.running {
				e.running = true
				e.previous = e.next

				s.wg.Add(1)

				go s.runJob(runCtx, e.id, e.job)
			}

			e.next = time.Time{}

			reschedule = append(reschedule, e)
		}

		// Collect the entries awaiting their first schedule evaluation —
		// newly added ones and every entry reset by [Scheduler.Stop].
		for _, e := range s.pending {
			e.queued = false

			reschedule = append(reschedule, e)
		}

		s.pending = nil

		s.mu.Unlock()

		// Compute the next activations WITHOUT holding the lock:
		// [Schedule.Next] is user code and may call back into the scheduler.
		updates := make([]nextActivation, 0, len(reschedule))

		for _, e := range reschedule {
			updates = append(updates, nextActivation{entry: e, next: e.schedule.Next(now)})
		}

		s.mu.Lock()

		// Apply the recomputed activations, skipping entries removed while
		// the lock was released.
		current := s.started && s.generation == generation && runCtx.Err() == nil

		if current {
			for _, u := range updates {
				e, ok := s.entries[u.entry.id]
				if !ok || e != u.entry {
					continue
				}

				e.next = u.next

				if !e.next.IsZero() {
					heap.Push(&s.queue, e)
				}
			}
		}

		var earliest time.Time

		if len(s.queue) > 0 {
			earliest = s.queue[0].next
		}

		s.mu.Unlock()

		var timer *time.Timer

		var timerC <-chan time.Time

		if !earliest.IsZero() {
			delay := earliest.Sub(s.now())

			if delay < 0 {
				delay = 0
			}

			// Cap the idle sleep so time sources substituted through
			// [WithClock] — which may jump forward arbitrarily — are
			// re-checked at least once per quantum.
			if delay > maxSleepQuantum {
				delay = maxSleepQuantum
			}

			timer = time.NewTimer(delay)
			timerC = timer.C
		}

		select {
		case <-runCtx.Done():
			if timer != nil {
				timer.Stop()
			}

			return
		case <-s.wake:
			if timer != nil {
				timer.Stop()
			}
		case <-timerC:
		}
	}
}

// runJob executes one run of the entry identified by id in its own
// goroutine, honoring the concurrency semaphore, recovering panics, and
// reporting failures to the configured [OnError] callback.
//
// Parameters:
//   - parent (context.Context): the scheduler run context the per-run
//     context derives from.
//   - id (int): the entry identifier (used to clear the in-flight flag).
//   - job (Job): the unit of work to execute.
func (s *Scheduler) runJob(parent context.Context, id int, job Job) {
	defer s.wg.Done()

	name, nameErr := safeJobName(job)
	if nameErr != nil {
		snapshot := s.finishRun(id, job)

		if s.onError != nil {
			deliverError(s.onError, &ErrorEvent{Entry: snapshot, Err: nameErr})
		}

		return
	}

	ctx, cancel := context.WithCancel(parent)

	defer cancel()

	if s.sem != nil {
		select {
		case s.sem <- struct{}{}:
			// The slot became available in the same instant shutdown
			// started. [Scheduler.Stop] cancels the run context after
			// clearing s.started, so re-check both before running: a run
			// admitted only after shutdown must not execute.
			if ctx.Err() != nil || !s.isStarted() {
				<-s.sem

				s.finishRun(id, job)

				return
			}

			defer func() { <-s.sem }()
		case <-ctx.Done():
			// Shutdown while waiting for a concurrency slot: release the
			// in-flight flag without running.
			s.finishRun(id, job)

			return
		}
	}

	if ctx.Err() != nil || !s.isStarted() {
		s.finishRun(id, job)

		return
	}

	err := runJobSafely(ctx, job, name)

	snapshot := s.finishRun(id, job)

	if err != nil && s.onError != nil {
		deliverError(s.onError, &ErrorEvent{Entry: snapshot, Err: err})
	}
}

// finishRun clears the in-flight flag of the entry identified by id and
// returns a snapshot of it for failure reporting. When the entry was removed
// while the run was in flight, the snapshot carries only the identifier and
// the job.
//
// Parameters:
//   - id (int): the entry identifier.
//   - job (Job): the job that ran, used for the fallback snapshot.
//
// Returns:
//   - snapshot (Entry): the entry snapshot.
func (s *Scheduler) finishRun(id int, job Job) (snapshot Entry) {
	snapshot = Entry{ID: id, Job: job}

	s.mu.Lock()

	defer s.mu.Unlock()

	if e, ok := s.entries[id]; ok {
		e.running = false

		snapshot = e.snapshot()
	}

	return
}

// isStarted reports whether the scheduling loop is currently running.
//
// Returns:
//   - started (bool): true while the scheduler is started.
func (s *Scheduler) isStarted() (started bool) {
	s.mu.Lock()

	defer s.mu.Unlock()

	started = s.started

	return
}

// ErrorEvent carries the data of one failed job run, delivered to the
// [OnError] callback configured with [WithOnError].
//
// Fields:
//   - Entry (Entry): snapshot of the failed entry. When the entry was removed
//     while the run was in flight, the snapshot carries only the identifier and
//     the job.
//   - Err (error): failure — the error the job returned, or its recovered
//     panic; never nil.
type ErrorEvent struct {
	Entry Entry
	Err   error
}

// OnError is the callback invoked for every failed job run, whether the
// job returned an error or panicked (panics are recovered and delivered as
// errors).
//
// Invocation is synchronous, on the goroutine that ran the job: a slow
// callback delays that goroutine's cleanup — and therefore [Scheduler.Stop],
// which waits for in-flight runs — but never the scheduling loop. The
// callback must be safe for concurrent use: runs of different entries
// execute concurrently even within a single scheduler.
//
// Failures include errors caused by [Scheduler.Stop] cancelling the per-run
// context (e.g. [context.Canceled] from jobs that honor it); filter them in
// the callback if shutdown noise is unwanted.
//
// A panicking callback is recovered and discarded, so a faulty observer
// cannot crash the scheduler process. A callback must not call
// [Scheduler.Stop] with a context that has no deadline: Stop waits for
// in-flight runs, including the run that invoked the callback, so the wait
// would deadlock. Use a context with a deadline, or perform shutdown outside
// the callback.
//
// Parameters:
//   - event (ErrorEvent): the failed run's entry snapshot and failure.
type OnError func(event ErrorEvent)

// Options configures a [Scheduler] and is populated through [OptionFunc]
// values passed to [NewScheduler].
//
// Fields:
//   - Location (*time.Location): cron evaluation location; defaults to
//     [time.Local].
//   - Clock (func() time.Time): time source; defaults to [time.Now].
//   - MaxConcurrency (int): maximum concurrent runs; zero means unlimited.
//   - OnError (OnError): failure callback; nil drops failures.
type Options struct {
	Location       *time.Location
	Clock          func() time.Time
	MaxConcurrency int
	OnError        OnError
}

// OptionFunc configures a [Scheduler] through the functional-options idiom.
// A nil OptionFunc is ignored. The With* helpers cover common settings, and
// callers may write custom options because [Options] is exported.
//
// Parameters:
//   - opts (*Options): the options to modify.
type OptionFunc func(opts *Options)

// maxSleepQuantum bounds how long the scheduling loop sleeps between
// re-evaluations, so substituted clocks that jump forward are honored
// promptly.
const maxSleepQuantum = time.Second

// NewScheduler creates a [Scheduler] configured by the given options.
//
// Parameters:
//   - ofs (...OptionFunc): functional options configuring the scheduler.
//
// Returns:
//   - scheduler (*Scheduler): a new scheduler, not yet started.
func NewScheduler(ofs ...OptionFunc) (scheduler *Scheduler) {
	opts := &Options{
		Location: time.Local,
		Clock:    time.Now,
	}

	for _, of := range ofs {
		if of != nil {
			of(opts)
		}
	}

	scheduler = &Scheduler{
		location: opts.Location,
		clock:    opts.Clock,
		onError:  opts.OnError,
		entries:  make(map[int]*entry),
		wake:     make(chan struct{}, 1),
	}

	if opts.MaxConcurrency > 0 {
		scheduler.sem = make(chan struct{}, opts.MaxConcurrency)
	}

	return
}

// WithLocation configures the location cron expressions are evaluated in.
//
// Parameters:
//   - loc (*time.Location): the evaluation location. Nil is ignored.
//
// Returns:
//   - fn (OptionFunc): the option applying loc.
func WithLocation(loc *time.Location) (fn OptionFunc) {
	fn = func(opts *Options) {
		if loc != nil {
			opts.Location = loc
		}
	}

	return
}

// WithClock configures the time source of the scheduler. It exists primarily
// as a test seam: substitute a fake clock to make scheduling deterministic.
//
// The clock must not call back into the scheduler: it is evaluated from the
// scheduling loop itself, and a re-entrant call can deadlock it (for example,
// [Scheduler.Stop] waits for the very loop that is evaluating the clock).
//
// Parameters:
//   - clock (func() time.Time): the time source. Nil is ignored.
//
// Returns:
//   - fn (OptionFunc): the option applying clock.
func WithClock(clock func() time.Time) (fn OptionFunc) {
	fn = func(opts *Options) {
		if clock != nil {
			opts.Clock = clock
		}
	}

	return
}

// WithMaxConcurrency bounds how many job runs may execute concurrently.
// Runs beyond the bound block on a semaphore until a slot frees up or the
// scheduler stops. A value of zero (the default) means unlimited concurrency.
//
// Parameters:
//   - n (int): the maximum number of concurrent runs. Negative values are
//     treated as zero.
//
// Returns:
//   - fn (OptionFunc): the option applying n.
func WithMaxConcurrency(n int) (fn OptionFunc) {
	fn = func(opts *Options) {
		if n < 0 {
			n = 0
		}

		opts.MaxConcurrency = n
	}

	return
}

// WithOnError configures the callback invoked for every failed job run,
// receiving an [ErrorEvent] with the entry snapshot and the failure
// (including recovered panics).
//
// Parameters:
//   - h (OnError): the failure callback. Nil drops failures silently.
//
// Returns:
//   - fn (OptionFunc): the option applying h.
func WithOnError(h OnError) (fn OptionFunc) {
	fn = func(opts *Options) {
		opts.OnError = h
	}

	return
}

// safeJobName resolves job.Name, converting a panic into an error so a broken
// Name implementation is reported through [OnError] instead of crashing the
// process.
//
// Parameters:
//   - job (Job): the job whose name to resolve.
//
// Returns:
//   - name (string): the job name, or its concrete type when Name panicked.
//   - err (error): non-nil when Name panicked.
func safeJobName(job Job) (name string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			name = fmt.Sprintf("%T", job)
			err = fmt.Errorf("scheduler.runJob: job name panicked: %v", recovered)
		}
	}()

	name = job.Name()

	return
}

// deliverError reports ev to h, recovering and discarding any panic so a
// faulty callback cannot crash the scheduler process.
//
// Parameters:
//   - h (OnError): the failure callback to invoke.
//   - ev (*ErrorEvent): the failed run's entry snapshot and failure.
func deliverError(h OnError, ev *ErrorEvent) {
	defer func() { _ = recover() }()

	h(*ev)
}

// runJobSafely executes job, converting a panic into an error so a panicking
// job never crashes the scheduler process.
//
// Parameters:
//   - ctx (context.Context): the per-run context.
//   - job (Job): the unit of work to execute.
//   - name (string): the job name, resolved by the caller so the panic path
//     never calls Name on possibly corrupted job state.
//
// Returns:
//   - err (error): the error the job returned, or the recovered panic.
func runJobSafely(ctx context.Context, job Job, name string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("scheduler.runJobSafely: job %q panicked: %v", name, recovered)
		}
	}()

	err = job.Run(ctx)

	return
}
