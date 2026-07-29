package scheduler

import (
	"time"
)

// Entry is a snapshot of a single job registered with a [Scheduler],
// capturing the job, its schedule, and the bookkeeping timestamps the
// scheduler maintains for introspection.
//
// Entry values are copies: mutating an [Entry] obtained from
// [Scheduler.Entries] never affects the scheduler's internal state.
//
// Fields:
//   - ID (int): the unique identifier assigned when the entry was registered;
//     pass it to [Scheduler.Remove] to unregister.
//   - Job (Job): the registered unit of work.
//   - Schedule (Schedule): the schedule the job fires on.
//   - Previous (time.Time): the time of the most recent activation, or the
//     zero [time.Time] when the job has not fired yet.
//   - Next (time.Time): the next scheduled activation, or the zero
//     [time.Time] when no future activation exists.
type Entry struct {
	ID       int
	Job      Job
	Schedule Schedule
	Previous time.Time
	Next     time.Time
}

// entry is the scheduler-internal mutable form of [Entry], adding the
// in-flight flag used to implement skip-overlap semantics.
//
// Fields:
//   - id (int): the unique entry identifier.
//   - job (Job): the registered unit of work.
//   - schedule (Schedule): the schedule the job fires on.
//   - previous (time.Time): the most recent activation time.
//   - next (time.Time): the next scheduled activation time.
//   - running (bool): whether a run of this entry is currently in flight.
type entry struct {
	id       int
	job      Job
	schedule Schedule
	previous time.Time
	next     time.Time
	running  bool
}

// snapshot copies the internal entry into an exported [Entry] value.
//
// Returns:
//   - o (Entry): an immutable-by-convention copy of the entry state.
func (e *entry) snapshot() (o Entry) {
	o = Entry{
		ID:       e.id,
		Job:      e.job,
		Schedule: e.schedule,
		Previous: e.previous,
		Next:     e.next,
	}

	return
}
