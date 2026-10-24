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
//     pass it to [Scheduler.Unregister] to unregister.
//   - Schedule (Schedule): the schedule the job fires on.
//   - Job (Job): the registered unit of work.
//   - Previous (time.Time): the time of the most recent activation, or the
//     zero [time.Time] when the job has not fired yet.
//   - Next (time.Time): the next scheduled activation, or the zero
//     [time.Time] when no future activation exists.
type Entry struct {
	ID       int
	Schedule Schedule
	Job      Job
	Previous time.Time
	Next     time.Time
}

// entry is the scheduler-internal mutable form of [Entry], adding the
// in-flight flag used to implement skip-overlap semantics and the
// bookkeeping the scheduling loop's min-heap requires.
//
// Fields:
//   - id (int): the unique entry identifier.
//   - schedule (Schedule): the schedule the job fires on.
//   - job (Job): the registered unit of work.
//   - previous (time.Time): the most recent activation time.
//   - next (time.Time): the next scheduled activation time.
//   - running (bool): whether a run of this entry is currently in flight.
//   - heapIndex (int): the entry's index in the scheduling heap, or -1 when
//     the entry is not in the heap.
//   - queued (bool): whether the entry awaits its first schedule evaluation
//     by the scheduling loop.
type entry struct {
	id        int
	schedule  Schedule
	job       Job
	previous  time.Time
	next      time.Time
	running   bool
	heapIndex int
	queued    bool
}

// snapshot copies the internal entry into an exported [Entry] value.
//
// Parameters:
//
//	none.
//
// Returns:
//   - o (Entry): an immutable-by-convention copy of the entry state.
func (e *entry) snapshot() (o Entry) {
	o = Entry{
		ID:       e.id,
		Schedule: e.schedule,
		Job:      e.job,
		Previous: e.previous,
		Next:     e.next,
	}

	return
}
