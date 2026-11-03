package scheduler

import (
	"context"
	"testing"
	"time"
)

// TestEntrySnapshotMapping covers the internal entry to Entry copy.
func TestEntrySnapshotMapping(t *testing.T) {
	t.Parallel()

	previous := time.Now()
	next := previous.Add(time.Hour)

	e := &entry{
		id:        7,
		schedule:  IntervalSchedule{Every: time.Hour},
		job:       NewJob("job", func(context.Context) error { return nil }),
		previous:  previous,
		next:      next,
		running:   true,
		heapIndex: 3,
		queued:    true,
	}

	snapshot := e.snapshot()

	if snapshot.ID != 7 || snapshot.Job == nil || snapshot.Schedule == nil {
		t.Errorf("snapshot() = %+v; want id 7 with job and schedule", snapshot)
	}

	if !snapshot.Previous.Equal(previous) || !snapshot.Next.Equal(next) {
		t.Errorf("snapshot() timestamps = %v/%v; want %v/%v", snapshot.Previous, snapshot.Next, previous, next)
	}

	// The snapshot carries no scheduling-loop bookkeeping.
	if snapshot.Next.IsZero() {
		t.Error("snapshot() zeroed the next activation")
	}
}
