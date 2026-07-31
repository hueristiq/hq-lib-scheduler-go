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

// maxSleepQuantum bounds how long the scheduling loop sleeps between
// re-evaluations, so substituted clocks that jump forward are honored
// promptly.
const maxSleepQuantum = time.Second

// ErrorHandler is the callback invoked for every failed job run, receiving a
// snapshot of the entry and the failure — a returned error or a recovered
// panic.
//
// Invocation is synchronous, on the goroutine that ran the job: a slow
// handler delays that goroutine's cleanup, never the scheduling loop. A
// handler shared across schedulers must be safe for concurrent use.
//
// Parameters:
//   - entry (Entry): a snapshot of the failed entry. When the entry was
//     removed while the run was in flight, the snapshot carries only the
//     identifier and the job.
//   - err (error): the failure; never nil.
type ErrorHandler func(entry Entry, err error)

// options holds the configurable knobs of a [Scheduler], populated through
// [OptionFunc] values passed to [NewScheduler].
//
// Fields:
//   - location (*time.Location): the location cron expressions are evaluated
//     in. Defaults to [time.Local].
//   - clock (func() time.Time): the time source. Defaults to [time.Now];
//     substitute it in tests to control scheduling deterministically.
//   - maxConcurrency (int): the maximum number of job runs executing
//     concurrently. Zero means unlimited.
//   - errorHandler (ErrorHandler): invoked with the entry snapshot and the
//     error (or recovered panic) of every failed run. Nil means failures are
//     silently dropped.
type options struct {
	location       *time.Location
	clock          func() time.Time
	maxConcurrency int
	errorHandler   ErrorHandler
}

// OptionFunc configures a [Scheduler] through the functional-options idiom.
//
// Parameters:
//   - opts (*options): the options bag to mutate.
type OptionFunc func(opts *options)

// WithLocation configures the location cron expressions are evaluated in.
//
// Parameters:
//   - loc (*time.Location): the evaluation location. Nil is ignored.
//
// Returns:
//   - of (OptionFunc): the option applying loc.
func WithLocation(loc *time.Location) (of OptionFunc) {
	of = func(opts *options) {
		if loc != nil {
			opts.location = loc
		}
	}

	return
}

// WithClock configures the time source of the scheduler. It exists primarily
// as a test seam: substitute a fake clock to make scheduling deterministic.
//
// Parameters:
//   - clock (func() time.Time): the time source. Nil is ignored.
//
// Returns:
//   - of (OptionFunc): the option applying clock.
func WithClock(clock func() time.Time) (of OptionFunc) {
	of = func(opts *options) {
		if clock != nil {
			opts.clock = clock
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
//   - of (OptionFunc): the option applying n.
func WithMaxConcurrency(n int) (of OptionFunc) {
	of = func(opts *options) {
		if n < 0 {
			n = 0
		}

		opts.maxConcurrency = n
	}

	return
}

// WithErrorHandler configures the callback invoked for every failed job run,
// receiving a snapshot of the entry and the failure (including recovered
// panics).
//
// Parameters:
//   - h (ErrorHandler): the failure callback. Nil drops failures silently.
//
// Returns:
//   - of (OptionFunc): the option applying h.
func WithErrorHandler(h ErrorHandler) (of OptionFunc) {
	of = func(opts *options) {
		opts.errorHandler = h
	}

	return
}

// Scheduler registers [Job] values against schedules and executes them
// concurrently on their activation times.
//
// # Lifecycle
//
//  1. Construct with [NewScheduler], optionally passing [OptionFunc] values.
//  2. Register jobs with [Scheduler.Add] (cron expression),
//     [Scheduler.AddEvery] (fixed interval), or [Scheduler.AddSchedule]
//     (custom [Schedule]); registration is possible both before and after
//     start.
//  3. Call [Scheduler.Start] to launch the scheduling loop (non-blocking).
//  4. Call [Scheduler.Stop] to shut down gracefully: the loop halts, per-run
//     contexts are cancelled, and in-flight runs are awaited.
//
// A stopped scheduler may be started again: [Scheduler.Start] after
// [Scheduler.Stop] launches a fresh loop and every entry is rescheduled from
// the current time.
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
// The zero value is not usable; construct through [NewScheduler].
//
// Fields:
//   - mu (sync.Mutex): guards every mutable field below.
//   - location (*time.Location): the location cron expressions are
//     evaluated in.
//   - clock (func() time.Time): the time source (test seam).
//   - maxConcurrency (int): the concurrency bound; zero means unlimited.
//   - sem (chan struct{}): the concurrency semaphore; nil when unlimited.
//   - errorHandler (ErrorHandler): the failure callback.
//   - entries (map[int]*entry): registered entries by identifier; the source
//     of truth for registration.
//   - queue (entryHeap): min-heap of the scheduled entries keyed on their
//     next activation, so the loop touches only due entries instead of
//     scanning every registered one. Holds exactly the entries with a
//     computed future activation.
//   - pending ([]*entry): registered entries awaiting their first schedule
//     evaluation by the loop — newly added ones, and every entry reset by
//     [Scheduler.Stop].
//   - nextID (int): the next entry identifier to hand out.
//   - generation (int): identifies the active scheduling loop; bumped by
//     every [Scheduler.Start] so a superseded loop exits instead of
//     double-firing jobs after a restart.
//   - started (bool): whether the scheduling loop is running.
//   - runCancel (context.CancelFunc): cancels the run context of the active
//     generation (and thus every per-run context derived from it).
//   - loopDone (chan struct{}): closed when the active scheduling loop exits.
//   - wake (chan struct{}): signals the loop to recompute its sleep.
//   - wg (sync.WaitGroup): tracks in-flight job runs for graceful stop.
type Scheduler struct {
	mu             sync.Mutex
	location       *time.Location
	clock          func() time.Time
	maxConcurrency int
	sem            chan struct{}
	errorHandler   ErrorHandler
	entries        map[int]*entry
	queue          entryHeap
	pending        []*entry
	nextID         int
	generation     int
	started        bool
	runCancel      context.CancelFunc
	loopDone       chan struct{}
	wake           chan struct{}
	wg             sync.WaitGroup
}

// NewScheduler creates a [Scheduler] configured by the given options.
//
// Parameters:
//   - ofs (...OptionFunc): functional options configuring the scheduler.
//
// Returns:
//   - scheduler (*Scheduler): a new scheduler, not yet started.
func NewScheduler(ofs ...OptionFunc) (scheduler *Scheduler) {
	opts := &options{
		location: time.Local,
		clock:    time.Now,
	}

	for _, of := range ofs {
		of(opts)
	}

	scheduler = &Scheduler{
		location:       opts.location,
		clock:          opts.clock,
		maxConcurrency: opts.maxConcurrency,
		errorHandler:   opts.errorHandler,
		entries:        make(map[int]*entry),
		wake:           make(chan struct{}, 1),
	}

	if opts.maxConcurrency > 0 {
		scheduler.sem = make(chan struct{}, opts.maxConcurrency)
	}

	return
}

// Add registers job to run on the schedule parsed from spec (see [Parse]).
//
// Parameters:
//   - spec (string): the cron expression, descriptor, or `@every` interval.
//   - job (Job): the unit of work to register; must not be nil.
//
// Returns:
//   - entryID (int): the identifier of the new entry, for [Scheduler.Remove].
//   - err (error): non-nil when spec fails to parse or job is nil.
func (s *Scheduler) Add(spec string, job Job) (entryID int, err error) {
	schedule, err := Parse(spec)
	if err != nil {
		err = fmt.Errorf("scheduler.Add: parsing %q: %w", spec, err)

		return
	}

	entryID, err = s.add(schedule, job)

	return
}

// AddEvery registers job to run at a fixed interval.
//
// Parameters:
//   - every (time.Duration): the interval between runs; must be positive.
//   - job (Job): the unit of work to register; must not be nil.
//
// Returns:
//   - entryID (int): the identifier of the new entry, for [Scheduler.Remove].
//   - err (error): non-nil when every is non-positive or job is nil.
func (s *Scheduler) AddEvery(every time.Duration, job Job) (entryID int, err error) {
	if every <= 0 {
		err = fmt.Errorf("scheduler.AddEvery: interval must be positive, got %s", every)

		return
	}

	entryID, err = s.add(IntervalSchedule{Every: every}, job)

	return
}

// AddSchedule registers job to run on a custom [Schedule] — for example one
// parsed once with [Parse] and shared between entries, or a user-defined
// [Schedule] implementation.
//
// Parameters:
//   - schedule (Schedule): the schedule the job fires on; must not be nil.
//   - job (Job): the unit of work to register; must not be nil.
//
// Returns:
//   - entryID (int): the identifier of the new entry, for [Scheduler.Remove].
//   - err (error): non-nil when schedule or job is nil.
func (s *Scheduler) AddSchedule(schedule Schedule, job Job) (entryID int, err error) {
	if schedule == nil {
		err = errors.New("scheduler.AddSchedule: schedule is nil")

		return
	}

	entryID, err = s.add(schedule, job)

	return
}

// add registers job under schedule and wakes the scheduling loop. The entry
// is queued for its first schedule evaluation rather than evaluated here:
// [Schedule.Next] is user code and never runs while holding s.mu.
//
// Parameters:
//   - schedule (Schedule): the schedule the job fires on.
//   - job (Job): the unit of work to register.
//
// Returns:
//   - entryID (int): the identifier of the new entry.
//   - err (error): non-nil when job is nil.
func (s *Scheduler) add(schedule Schedule, job Job) (entryID int, err error) {
	if job == nil {
		err = errors.New("scheduler.add: job is nil")

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

// Remove unregisters the entry with the given identifier. Removing an
// unknown identifier is a no-op. An in-flight run of the removed entry is
// not interrupted.
//
// Parameters:
//   - entryID (int): the identifier returned by [Scheduler.Add],
//     [Scheduler.AddEvery], or [Scheduler.AddSchedule].
func (s *Scheduler) Remove(entryID int) {
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
// immediately. Calling Start on an already-started scheduler is a no-op;
// calling it after [Scheduler.Stop] starts a fresh loop.
//
// The provided context becomes the parent of every per-run context:
// cancelling it stops the loop and cancels in-flight runs, exactly like
// [Scheduler.Stop].
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

	go s.loop(generation, runCtx, loopDone)
}

// Stop gracefully shuts the scheduler down: the scheduling loop halts, every
// per-run context is cancelled, and in-flight runs are awaited until they
// finish or ctx expires. Calling Stop on a stopped scheduler is a no-op.
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
		err = fmt.Errorf("scheduler.Stop: timed out waiting for the scheduling loop: %w", ctx.Err())

		return
	}

	waited := make(chan struct{})

	go func() {
		s.wg.Wait()

		close(waited)
	}()

	select {
	case <-waited:
	case <-ctx.Done():
		err = fmt.Errorf("scheduler.Stop: timed out waiting for in-flight runs: %w", ctx.Err())

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

// nextActivation carries an entry's freshly computed next activation from
// the scheduling loop's unlocked evaluation phase back into its critical
// section.
//
// Fields:
//   - entry (*entry): the entry the activation was computed for.
//   - next (time.Time): the activation computed by the entry's schedule.
type nextActivation struct {
	entry *entry
	next  time.Time
}

// entryHeap is a min-heap of entries keyed on their next activation time,
// implementing [heap.Interface]. It is not safe for concurrent use on its
// own: every operation happens under [Scheduler.mu].
type entryHeap []*entry

// Len returns the number of entries in the heap.
//
// Returns:
//   - n (int): the heap size.
func (h entryHeap) Len() (n int) {
	n = len(h)

	return
}

// Less reports whether the entry at i activates before the entry at j,
// breaking activation-time ties by entry identifier for determinism.
//
// Parameters:
//   - i (int): the index of the first entry.
//   - j (int): the index of the second entry.
//
// Returns:
//   - less (bool): true when the entry at i orders before the entry at j.
func (h entryHeap) Less(i, j int) (less bool) {
	if h[i].next.Equal(h[j].next) {
		less = h[i].id < h[j].id

		return
	}

	less = h[i].next.Before(h[j].next)

	return
}

// Swap swaps the entries at i and j, keeping their heap indices in sync.
//
// Parameters:
//   - i (int): the index of the first entry.
//   - j (int): the index of the second entry.
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

// Push appends x, which must be an [*entry], to the heap.
//
// Parameters:
//   - x (any): the entry to append.
func (h *entryHeap) Push(x any) {
	e := x.(*entry)

	e.heapIndex = len(*h)

	*h = append(*h, e)
}

// Pop removes and returns the last entry of the heap; [heap.Pop] moves the
// minimum to the end before calling it.
//
// Returns:
//   - x (any): the removed entry.
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
//   - generation (int): the generation this loop belongs to.
//   - runCtx (context.Context): the run context of this generation.
//   - loopDone (chan struct{}): closed when the loop exits.
func (s *Scheduler) loop(generation int, runCtx context.Context, loopDone chan struct{}) {
	defer close(loopDone)

	for {
		s.mu.Lock()

		if !s.started || s.generation != generation || runCtx.Err() != nil {
			s.mu.Unlock()

			return
		}

		now := s.now()

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

				go s.runJob(e.id, e.job, runCtx)
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

		for _, u := range updates {
			if !current {
				break
			}

			e, ok := s.entries[u.entry.id]
			if !ok || e != u.entry {
				continue
			}

			e.next = u.next

			if !e.next.IsZero() {
				heap.Push(&s.queue, e)
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
// reporting failures to the configured error handler.
//
// Parameters:
//   - id (int): the entry identifier (used to clear the in-flight flag).
//   - job (Job): the unit of work to execute.
//   - parent (context.Context): the scheduler run context the per-run
//     context derives from.
func (s *Scheduler) runJob(id int, job Job, parent context.Context) {
	defer s.wg.Done()

	name := job.Name()

	ctx, cancel := context.WithCancel(parent)

	defer cancel()

	if s.sem != nil {
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		case <-ctx.Done():
			// Shutdown while waiting for a concurrency slot: release the
			// in-flight flag without running.
			s.finishRun(id, job)

			return
		}
	}

	err := runJobSafely(ctx, job, name)

	snapshot := s.finishRun(id, job)

	if err != nil && s.errorHandler != nil {
		s.errorHandler(snapshot, err)
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
			err = fmt.Errorf("scheduler.runJob: job %q panicked: %v", name, recovered)
		}
	}()

	err = job.Run(ctx)

	return
}
