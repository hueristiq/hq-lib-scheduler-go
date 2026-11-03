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

// Scheduler runs jobs on their schedules, concurrently, with graceful
// shutdown through [Scheduler.Stop]. Construct it with [New]; the zero value
// is not usable.
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

// ErrorEvent reports a job failure to [WithOnError].
//
// Fields:
//   - Entry (Entry): the failing entry's snapshot.
//   - Err (error): the failure.
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
// values passed to [New].
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

// New creates a [Scheduler] configured by the given options.
//
// Invalid values are normalized: a nil Location falls back to [time.Local] and
// a nil Clock falls back to [time.Now], so custom [OptionFunc] values cannot
// configure a scheduler that panics on first use.
//
// Parameters:
//   - ofs (...OptionFunc): functional options configuring the scheduler.
//
// Returns:
//   - scheduler (*Scheduler): a new scheduler, not yet started.
func New(ofs ...OptionFunc) (scheduler *Scheduler) {
	opts := &Options{
		Location: time.Local,
		Clock:    time.Now,
	}

	for _, of := range ofs {
		if of != nil {
			of(opts)
		}
	}

	if opts.Location == nil {
		opts.Location = time.Local
	}

	if opts.Clock == nil {
		opts.Clock = time.Now
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

// RegisterFunc registers run as a job named name on the schedule parsed from
// spec (see [Parse]). It is a convenience wrapper: it wraps run with [NewJob]
// and delegates to [Scheduler.Register].
//
// Parameters:
//   - spec (string): the cron expression, descriptor, or `@every` interval.
//   - name (string): the human-readable job name, used for introspection and
//     error reporting.
//   - run (JobFunc): the function executed on each run.
//
// Returns:
//   - entryID (int): the identifier of the new entry, for [Scheduler.Unregister].
//   - err (error): non-nil when spec fails to parse.
func (s *Scheduler) RegisterFunc(spec, name string, run JobFunc) (entryID int, err error) {
	entryID, err = s.Register(spec, NewJob(name, run))

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
