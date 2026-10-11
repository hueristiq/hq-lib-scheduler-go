package scheduler

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRunJob_DoesNotRunWhenStopped covers the shutdown re-check in runJob:
// after acquiring a concurrency slot, a run must bail out without executing
// when the scheduler is no longer started — the window where
// [Scheduler.Stop] has cleared the started flag but not yet cancelled the
// run context.
func TestRunJob_DoesNotRunWhenStopped(t *testing.T) {
	t.Parallel()

	s := NewScheduler(WithMaxConcurrency(1))

	var runs atomic.Int32

	job := NewJob("never", JobFunc(func(ctx context.Context) error {
		runs.Add(1)

		return nil
	}))

	id := 1

	s.mu.Lock()

	s.entries[id] = &entry{
		id:        id,
		job:       job,
		heapIndex: -1,
	}

	s.mu.Unlock()

	s.wg.Add(1)

	s.runJob(id, job, context.Background())

	s.wg.Wait()

	require.Zero(t, runs.Load())
}
