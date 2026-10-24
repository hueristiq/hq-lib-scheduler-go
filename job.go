package scheduler

import (
	"context"
)

// Job is the unit of work executed by a [Scheduler].
//
// Implementations must be safe for sequential execution; the scheduler
// guarantees that runs of the same entry never overlap (see [Scheduler]).
// Runs of different entries execute concurrently, so a job registered more
// than once — or one sharing state with other jobs — must synchronize that
// state itself.
type Job interface {
	// Name returns the human-readable name of the job, used for
	// introspection and error reporting.
	//
	// Parameters:
	//
	//	none.
	//
	// Returns:
	//   - name (string): the job name.
	Name() (name string)

	// Run executes the job once.
	//
	// The provided context is cancelled when the owning [Scheduler] is
	// stopped; implementations should honor it for prompt shutdown.
	//
	// Parameters:
	//   - ctx (context.Context): the per-run context, cancelled on
	//     [Scheduler.Stop].
	//
	// Returns:
	//   - err (error): non-nil when the run fails; the error is passed to the
	//     scheduler's configured error handler.
	Run(ctx context.Context) (err error)
}

// JobFunc adapts a plain function into the run method of a [Job]; see
// [NewJob].
//
// Parameters:
//   - ctx (context.Context): the per-run context, cancelled on
//     [Scheduler.Stop].
//
// Returns:
//   - err (error): whatever the wrapped function returns.
type JobFunc func(ctx context.Context) (err error)

// Run executes the wrapped function.
//
// Parameters:
//   - ctx (context.Context): the per-run context, cancelled on
//     [Scheduler.Stop].
//
// Returns:
//   - err (error): whatever the wrapped function returns.
func (f JobFunc) Run(ctx context.Context) (err error) {
	err = f(ctx)

	return
}

// namedJob is the [Job] implementation returned by [NewJob].
//
// Fields:
//   - name (string): the human-readable job name.
//   - run (JobFunc): the function executed on each run.
type namedJob struct {
	name string
	run  JobFunc
}

// Name returns the name the job was constructed with.
//
// Parameters:
//
//	none.
//
// Returns:
//   - name (string): the job name.
func (j *namedJob) Name() (name string) {
	name = j.name

	return
}

// Run executes the wrapped [JobFunc].
//
// Parameters:
//   - ctx (context.Context): the per-run context, cancelled on
//     [Scheduler.Stop].
//
// Returns:
//   - err (error): whatever the wrapped function returns.
func (j *namedJob) Run(ctx context.Context) (err error) {
	err = j.run(ctx)

	return
}

// NewJob adapts the run function into a named [Job].
//
// Parameters:
//   - name (string): the human-readable job name, used for introspection and
//     error reporting.
//   - run (JobFunc): the function executed on each run; must not be nil.
//
// Returns:
//   - job (Job): a [Job] wrapping run under name.
func NewJob(name string, run JobFunc) (job Job) {
	job = &namedJob{
		name: name,
		run:  run,
	}

	return
}

// Compile-time guard ensuring [namedJob] satisfies [Job]. JobFunc deliberately
// does not: it adapts a plain function into the Run method only; [NewJob]
// pairs it with a name.
var _ Job = (*namedJob)(nil)
