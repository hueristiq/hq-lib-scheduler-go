package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	hqgoschedulerlogger "github.com/hueristiq/hq-lib-scheduler-go/logger"
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
//   - logger (hqgoschedulerlogger.Logger): the structured logger receiving
//     lifecycle, activation, and failure records. Defaults to [logger.Nop].
type options struct {
	location       *time.Location
	clock          func() time.Time
	maxConcurrency int
	errorHandler   ErrorHandler
	logger         hqgoschedulerlogger.Logger
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

// WithLogger configures the structured logger receiving the scheduler's
// lifecycle, activation, and failure records. A panic from the logger is
// recovered: a misbehaving logger never crashes the scheduler.
//
// Parameters:
//   - l (hqgoschedulerlogger.Logger): the logger. Nil is ignored.
//
// Returns:
//   - of (OptionFunc): the option applying l.
func WithLogger(l hqgoschedulerlogger.Logger) (of OptionFunc) {
	of = func(opts *options) {
		if l != nil {
			opts.logger = l
		}
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
//   - logger (hqgoschedulerlogger.Logger): the structured logger receiving
//     lifecycle, activation, and failure records.
//   - entries (map[int]*entry): registered entries by identifier.
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
	logger         hqgoschedulerlogger.Logger
	entries        map[int]*entry
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

	if opts.logger == nil {
		opts.logger = hqgoschedulerlogger.Nop{}
	}

	// Guard every record against a panicking logger.
	opts.logger = safeLogger{logger: opts.logger}

	scheduler = &Scheduler{
		location:       opts.location,
		clock:          opts.clock,
		maxConcurrency: opts.maxConcurrency,
		errorHandler:   opts.errorHandler,
		logger:         opts.logger,
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

// add registers job under schedule and wakes the scheduling loop.
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

	s.entries[entryID] = &entry{
		id:       entryID,
		job:      job,
		schedule: schedule,
	}

	s.mu.Unlock()

	s.logger.Debug("entry added", "entry_id", entryID, "job", job.Name())

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

	_, existed := s.entries[entryID]

	delete(s.entries, entryID)

	s.mu.Unlock()

	if existed {
		s.logger.Debug("entry removed", "entry_id", entryID)
	}

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

	defer s.mu.Unlock()

	entries = make([]Entry, 0, len(s.entries))

	for _, e := range s.entries {
		entries = append(entries, e.snapshot())
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })

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

	s.logger.Debug("scheduler started")
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

		s.logger.Warn("scheduler stop timed out waiting for the scheduling loop", "error", err)

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

		s.logger.Warn("scheduler stop timed out waiting for in-flight runs", "error", err)

		return
	}

	s.mu.Lock()

	for _, e := range s.entries {
		e.running = false
		e.next = time.Time{}
	}

	s.mu.Unlock()

	s.logger.Debug("scheduler stopped")

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

// skippedActivation records an activation dropped because the previous run
// of its entry was still in flight, so it can be logged after the scheduling
// loop releases its lock.
//
// Fields:
//   - id (int): the entry identifier.
//   - job (Job): the job whose run was skipped; its name is resolved after
//     the lock is released.
//   - scheduled (time.Time): the dropped activation time.
type skippedActivation struct {
	id        int
	job       Job
	scheduled time.Time
}

// loop is the scheduling loop: it fires due entries, then sleeps until the
// earliest next activation, a wake-up signal, or shutdown.
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

		var earliest time.Time

		var skipped []skippedActivation

		for _, e := range s.entries {
			if e.next.IsZero() {
				e.next = e.schedule.Next(now)
			}

			if !e.next.IsZero() && !e.next.After(now) {
				if !e.running {
					e.running = true
					e.previous = e.next

					s.wg.Add(1)

					go s.runJob(e.id, e.job, runCtx)
				} else {
					skipped = append(skipped, skippedActivation{
						id:        e.id,
						job:       e.job,
						scheduled: e.next,
					})
				}

				// Reschedule from now; an activation skipped because the
				// previous run is still in flight is dropped.
				e.next = e.schedule.Next(now)
			}

			if !e.next.IsZero() && (earliest.IsZero() || e.next.Before(earliest)) {
				earliest = e.next
			}
		}

		s.mu.Unlock()

		for _, skip := range skipped {
			s.logger.Info("job run skipped: previous run still in flight",
				"entry_id", skip.id,
				"job", skip.job.Name(),
				"scheduled", skip.scheduled,
			)
		}

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

			s.logger.Debug("job run cancelled before start", "entry_id", id, "job", name)

			return
		}
	}

	s.logger.Debug("job run started", "entry_id", id, "job", name)

	started := s.now()

	err := runJobSafely(ctx, job)

	duration := s.now().Sub(started)

	snapshot := s.finishRun(id, job)

	if err != nil {
		s.logger.Error("job run failed", "entry_id", id, "job", name, "error", err, "duration", duration)

		if s.errorHandler != nil {
			s.errorHandler(snapshot, err)
		}
	} else {
		s.logger.Debug("job run finished", "entry_id", id, "job", name, "duration", duration)
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
//
// Returns:
//   - err (error): the error the job returned, or the recovered panic.
func runJobSafely(ctx context.Context, job Job) (err error) {
	// Resolve the name up front so the panic path never calls Name on
	// possibly corrupted job state.
	name := job.Name()

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("scheduler.runJob: job %q panicked: %v", name, recovered)
		}
	}()

	err = job.Run(ctx)

	return
}

// safeLogger wraps a [hqgoschedulerlogger.Logger], recovering a panic from
// any of its methods: a misbehaving logger never crashes the scheduler or
// its caller; the panicking record is dropped.
//
// Fields:
//   - logger (hqgoschedulerlogger.Logger): the wrapped logger.
type safeLogger struct {
	logger hqgoschedulerlogger.Logger
}

// Debug emits a debug record through the wrapped logger.
//
// Parameters:
//   - msg (string): the log message.
//   - args (...any): alternating keys and values annotating the record.
func (s safeLogger) Debug(msg string, args ...any) {
	defer func() {
		_ = recover()
	}()

	s.logger.Debug(msg, args...)
}

// Info emits an informational record through the wrapped logger.
//
// Parameters:
//   - msg (string): the log message.
//   - args (...any): alternating keys and values annotating the record.
func (s safeLogger) Info(msg string, args ...any) {
	defer func() {
		_ = recover()
	}()

	s.logger.Info(msg, args...)
}

// Warn emits a warning record through the wrapped logger.
//
// Parameters:
//   - msg (string): the log message.
//   - args (...any): alternating keys and values annotating the record.
func (s safeLogger) Warn(msg string, args ...any) {
	defer func() {
		_ = recover()
	}()

	s.logger.Warn(msg, args...)
}

// Error emits a failure record through the wrapped logger.
//
// Parameters:
//   - msg (string): the log message.
//   - args (...any): alternating keys and values annotating the record.
func (s safeLogger) Error(msg string, args ...any) {
	defer func() {
		_ = recover()
	}()

	s.logger.Error(msg, args...)
}
