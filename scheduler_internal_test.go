package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestJobFuncRun(t *testing.T) {
	t.Parallel()

	called := false

	job := JobFunc(func(context.Context) error {
		called = true

		return nil
	})

	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if !called {
		t.Fatal("Run() did not call the wrapped function")
	}
}

func TestParseFieldEdges(t *testing.T) {
	t.Parallel()

	cases := []string{"", "1-x", "x-2", "1-2x", "1-99", "99"}

	for _, field := range cases {
		if _, err := parseField(field, 0, 59, nil, false, "minute"); err == nil {
			t.Fatalf("parseField(%q) error = nil", field)
		}
	}

	if _, err := parseNumber("999999999999999999999", nil); err == nil {
		t.Fatal("parseNumber(overflow) error = nil")
	}
}

func TestWithMaxConcurrencyNegative(t *testing.T) {
	t.Parallel()

	opts := &options{}
	WithMaxConcurrency(-1)(opts)

	if opts.maxConcurrency != 0 {
		t.Fatalf("maxConcurrency = %d, want 0", opts.maxConcurrency)
	}
}

func TestStopCanceledContext(t *testing.T) {
	t.Parallel()

	scheduler := &Scheduler{
		started:   true,
		runCancel: func() {},
		loopDone:  make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := scheduler.Stop(ctx); err == nil {
		t.Fatal("Stop(canceled) error = nil")
	}
}

type scriptedSchedule struct {
	mu                 sync.Mutex
	calls              int
	scheduler          *Scheduler
	entryID            int
	done               chan struct{}
	unregisterOnSecond bool
}

func (s *scriptedSchedule) Next(after time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++

	switch s.calls {
	case 1:
		return after.Add(10 * time.Millisecond)
	case 2:
		if s.unregisterOnSecond {
			s.scheduler.Unregister(s.entryID)
			close(s.done)

			return time.Time{}
		}

		return after.Add(-time.Second)
	default:
		close(s.done)

		return time.Time{}
	}
}

func TestLoopRescheduleBranches(t *testing.T) {
	t.Parallel()

	scheduler := NewScheduler()
	removed := &scriptedSchedule{scheduler: scheduler, done: make(chan struct{}), unregisterOnSecond: true}
	past := &scriptedSchedule{scheduler: scheduler, done: make(chan struct{})}

	job := NewJob("noop", func(context.Context) error {
		return nil
	})

	removedID, err := scheduler.RegisterSchedule(removed, job)
	if err != nil {
		t.Fatalf("RegisterSchedule(removed) error = %v", err)
	}

	pastID, err := scheduler.RegisterSchedule(past, job)
	if err != nil {
		t.Fatalf("RegisterSchedule(past) error = %v", err)
	}

	removed.entryID = removedID
	past.entryID = pastID

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler.Start(ctx)

	for name, done := range map[string]<-chan struct{}{"removed": removed.done, "past": past.done} {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s schedule timed out", name)
		}
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()

	_ = scheduler.Stop(stopCtx)
}
