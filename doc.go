// Package scheduler runs jobs on cron expressions, named descriptors, or fixed intervals,
// concurrently and with graceful shutdown.
//
// It suits background work that must happen on a timetable — report generation, cache
// refreshes, periodic cleanup — where a process-local, dependency-free scheduler is more
// useful than an external cron daemon.
//
// # Schedules
//
// A [Schedule] answers a single question: given an instant, when is the next activation
// strictly after it. [Parse] builds a Schedule from a standard 5-field cron expression
// (`minute hour day-of-month month day-of-week`, supporting `*`, `*/n`, ranges, lists,
// month and day names, and `?` as a wildcard alias in the day fields), from a descriptor
// (`@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`), or
// from `@every <duration>`, which yields an [IntervalSchedule].
//
// # Jobs
//
// A [Job] is the unit of work: a human-readable name and a Run method receiving a per-run
// context. [NewJob] adapts a plain [JobFunc] function into a named Job.
//
// # The Scheduler
//
// [NewScheduler] creates a [Scheduler]. [Scheduler.Register] registers a job on a cron
// expression, descriptor, or `@every` interval; [Scheduler.RegisterSchedule] accepts a
// ready-made [Schedule], including custom implementations. Registration is possible
// both before and after start.
// [Scheduler.Start] launches the scheduling loop in its own goroutine, and
// [Scheduler.Stop] shuts it down gracefully: the loop halts, every per-run context is
// cancelled, and in-flight runs are awaited. [Scheduler.Entries] returns a snapshot of
// the registered jobs and their bookkeeping for introspection.
//
// # Run semantics
//
// Each activation fires its job in its own goroutine, bounded by the [WithMaxConcurrency]
// semaphore. Overlapping runs of the same entry are skipped: if the previous run is still
// in flight when the next activation arrives, that activation is dropped and the entry is
// rescheduled. A panicking job is recovered and reported — together with returned
// errors — to the [WithOnError] callback.
//
// # Configuration
//
// Behavior is set through functional options passed to [NewScheduler]:
//
//   - [WithLocation] sets the location cron expressions are evaluated in.
//   - [WithClock] substitutes the time source, primarily as a test seam.
//   - [WithMaxConcurrency] bounds how many job runs execute concurrently.
//   - [WithOnError] registers the callback invoked for every failed run.
//
// # Concurrency
//
// A Scheduler is safe for concurrent use: jobs may be registered, removed, and inspected
// from any goroutine while the loop runs. Job Run methods execute on their own
// goroutines; runs of the same entry never overlap, but different entries run
// concurrently, so jobs sharing state must synchronize it themselves.
//
// The implementation relies on the Go standard library only.
package scheduler
