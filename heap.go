package scheduler

import (
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
//  1. Construct with [New], optionally passing [OptionFunc] values.
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
// The zero value is not usable; construct through [New]. A Scheduler must
// not be copied after first use.
//
// Fields:
