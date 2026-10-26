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
//   - ID (int): unique identifier assigned when the entry was registered;
//     pass it to [Scheduler.Unregister].
//   - Schedule (Schedule): schedule the job fires on.
//   - Job (Job): registered unit of work.
//   - Previous (time.Time): time of the most recent activation, or the zero
//     [time.Time] when the job has not fired yet.
//   - Next (time.Time): next scheduled activation; the zero [time.Time] when no
//     future activation exists or the entry has not been evaluated yet.
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
//   - id (int): unique identifier.
//   - schedule (Schedule): activation schedule.
//   - job (Job): unit of work.
//   - previous (time.Time): most recent activation.
//   - next (time.Time): next scheduled activation.
//   - running (bool): whether a run is in flight.
//   - heapIndex (int): index in the scheduling heap; -1 when absent.
//   - queued (bool): awaiting first schedule evaluation.
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
