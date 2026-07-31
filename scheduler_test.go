package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

// fakeClock is a manually advanced [time.Time] source used through
// [hqgoscheduler.WithClock].
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// newFakeClock creates a fake clock starting at the given instant.
func newFakeClock(start time.Time) (clock *fakeClock) {
	clock = &fakeClock{now: start}

	return
}

// Now returns the current fake time.
func (c *fakeClock) Now() (now time.Time) {
	c.mu.Lock()

	defer c.mu.Unlock()

	now = c.now

	return
}

// Advance moves the fake time forward by d.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()

	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

func TestScheduler_RegisterAndEntries(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Date(2025, time.January, 15, 10, 30, 0, 0, time.UTC))

	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithClock(clock.Now),
	)

	job := hqgoscheduler.NewJob("heartbeat", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	}))

	id1, err := scheduler.Register("0 12 * * *", job)
	require.NoError(t, err)
	require.Positive(t, id1)

	id2, err := scheduler.Register("@every 1m", job)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	scheduler.Start(t.Context())

	defer func() {
		require.NoError(t, scheduler.Stop(t.Context()))
	}()

	require.Eventually(t, func() (ok bool) {
		entries := scheduler.Entries()

		if len(entries) != 2 {
			return
		}

		ok = !entries[0].Next.IsZero() && !entries[1].Next.IsZero()

		return
	}, 2*time.Second, 5*time.Millisecond)

	entries := scheduler.Entries()
	require.Equal(t, id1, entries[0].ID)
	require.Equal(t, id2, entries[1].ID)
	require.Equal(t, "heartbeat", entries[0].Job.Name())
	require.Equal(t, time.Date(2025, time.January, 15, 12, 0, 0, 0, time.UTC), entries[0].Next)
	require.Equal(t, clock.Now().Add(time.Minute), entries[1].Next)

	scheduler.Unregister(id1)

	entries = scheduler.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, id2, entries[0].ID)

	// Unregistering an unknown identifier is a no-op.
	scheduler.Unregister(9999)
	require.Len(t, scheduler.Entries(), 1)
}

func TestScheduler_RegisterValidation(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	job := hqgoscheduler.NewJob("job", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	}))

	_, err := scheduler.Register("not a spec", job)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scheduler.Register")

	_, err = scheduler.Register("0 12 * * *", nil)
	require.Error(t, err)
}

func TestScheduler_RunsJobs(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	var runs atomic.Int32

	_, err := scheduler.Register("@every 20ms", hqgoscheduler.NewJob("counter", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		runs.Add(1)

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	require.Eventually(t, func() (ok bool) {
		ok = runs.Load() >= 3

		return
	}, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))

	entries := scheduler.Entries()
	require.Len(t, entries, 1)
	require.False(t, entries[0].Previous.IsZero())
}

func TestScheduler_SkipOverlap(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	var (
		runs        atomic.Int32
		maxInFlight atomic.Int32
		inFlight    atomic.Int32
	)

	_, err := scheduler.Register("@every 10ms", hqgoscheduler.NewJob("slow", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		current := inFlight.Add(1)

		for {
			peak := maxInFlight.Load()

			if current <= peak || maxInFlight.CompareAndSwap(peak, current) {
				break
			}
		}

		time.Sleep(60 * time.Millisecond)

		inFlight.Add(-1)

		runs.Add(1)

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	time.Sleep(350 * time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))

	// Overlapping activations are skipped: never two in flight, and far fewer
	// completions than the 35 activations a naive runner would have started.
	require.Equal(t, int32(1), maxInFlight.Load())
	require.Less(t, runs.Load(), int32(10))
	require.GreaterOrEqual(t, runs.Load(), int32(1))
}

func TestScheduler_OnError(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		failures []error
		entries  []hqgoscheduler.Entry
	)

	scheduler := hqgoscheduler.NewScheduler(hqgoscheduler.WithOnError(func(ev hqgoscheduler.ErrorEvent) {
		mu.Lock()

		defer mu.Unlock()

		failures = append(failures, ev.Err)
		entries = append(entries, ev.Entry)
	}))

	boom := errors.New("boom")

	_, err := scheduler.Register("@every 15ms", hqgoscheduler.NewJob("failing", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		err = boom

		return
	})))
	require.NoError(t, err)

	_, err = scheduler.Register("@every 15ms", hqgoscheduler.NewJob("panicking", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		panic("kaput")
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	require.Eventually(t, func() (ok bool) {
		mu.Lock()

		defer mu.Unlock()

		ok = len(failures) >= 4

		return
	}, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))

	mu.Lock()

	defer mu.Unlock()

	var sawBoom, sawPanic bool

	for _, failure := range failures {
		if errors.Is(failure, boom) {
			sawBoom = true
		}

		if failure != nil && !errors.Is(failure, boom) {
			sawPanic = true
		}
	}

	require.True(t, sawBoom)
	require.True(t, sawPanic)
	require.NotEmpty(t, entries)
	require.NotEmpty(t, entries[0].Job.Name())
}

func TestScheduler_OnErrorPanicIsRecovered(t *testing.T) {
	t.Parallel()

	var runs atomic.Int32

	scheduler := hqgoscheduler.NewScheduler(hqgoscheduler.WithOnError(func(_ hqgoscheduler.ErrorEvent) {
		panic("observer kaput")
	}))

	_, err := scheduler.Register("@every 10ms", hqgoscheduler.NewJob("failing", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		runs.Add(1)

		err = errors.New("boom")

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	time.Sleep(50 * time.Millisecond)

	// Reaching Stop at all proves the panicking callback was recovered: an
	// unrecovered panic would kill the test binary.
	require.NoError(t, scheduler.Stop(t.Context()))

	// The scheduling loop was unaffected: the failing job kept firing.
	require.Greater(t, runs.Load(), int32(1))
}

func TestScheduler_StopCancelsPerRunContext(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	started := make(chan struct{})

	_, err := scheduler.Register("@every 10ms", hqgoscheduler.NewJob("blocking", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		select {
		case <-started:
		default:
			close(started)
		}

		<-ctx.Done()

		err = ctx.Err()

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("job never started")
	}

	// The job only returns when its per-run context is cancelled; Stop must
	// cancel it and then complete.
	require.NoError(t, scheduler.Stop(t.Context()))
}

func TestScheduler_StopHonorsContextDeadline(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	started := make(chan struct{})

	_, err := scheduler.Register("@every 10ms", hqgoscheduler.NewJob("stubborn", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		select {
		case <-started:
		default:
			close(started)
		}

		// Deliberately ignores ctx cancellation.
		time.Sleep(5 * time.Second)

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("job never started")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err = scheduler.Stop(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestScheduler_MaxConcurrency(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler(hqgoscheduler.WithMaxConcurrency(1))

	var (
		inFlight    atomic.Int32
		maxInFlight atomic.Int32
	)

	work := hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		current := inFlight.Add(1)

		for {
			peak := maxInFlight.Load()

			if current <= peak || maxInFlight.CompareAndSwap(peak, current) {
				break
			}
		}

		time.Sleep(40 * time.Millisecond)

		inFlight.Add(-1)

		return
	})

	_, err := scheduler.Register("@every 10ms", hqgoscheduler.NewJob("one", work))
	require.NoError(t, err)

	_, err = scheduler.Register("@every 10ms", hqgoscheduler.NewJob("two", work))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	time.Sleep(300 * time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))

	require.Equal(t, int32(1), maxInFlight.Load())
}

func TestScheduler_StartStopIdempotent(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	// Double start is a no-op.
	scheduler.Start(t.Context())
	scheduler.Start(t.Context())

	require.NoError(t, scheduler.Stop(t.Context()))

	// Double stop is a no-op.
	require.NoError(t, scheduler.Stop(t.Context()))
}

func TestScheduler_Restart(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	var runs atomic.Int32

	_, err := scheduler.Register("@every 15ms", hqgoscheduler.NewJob("counter", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		runs.Add(1)

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	require.Eventually(t, func() (ok bool) {
		ok = runs.Load() >= 2

		return
	}, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))

	// A stopped scheduler can be started again: the entry is rescheduled and
	// fires on the fresh loop.
	scheduler.Start(t.Context())

	require.Eventually(t, func() (ok bool) {
		ok = runs.Load() >= 4

		return
	}, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))
}

func TestScheduler_StartWithCancelledContext(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	var runs atomic.Int32

	_, err := scheduler.Register("@every 10ms", hqgoscheduler.NewJob("never", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		runs.Add(1)

		return
	})))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	scheduler.Start(ctx)

	// The loop exits immediately on a cancelled lifecycle context; give it a
	// moment to prove nothing fires, then shut down cleanly.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(0), runs.Load())

	require.NoError(t, scheduler.Stop(t.Context()))
}

func TestScheduler_FiresCronExpressionWithFakeClock(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, time.January, 15, 10, 29, 59, 0, time.UTC)

	clock := newFakeClock(start)

	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithClock(clock.Now),
	)

	fired := make(chan struct{}, 1)

	_, err := scheduler.Register("*/1 * * * *", hqgoscheduler.NewJob("minutely", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		select {
		case fired <- struct{}{}:
		default:
		}

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	defer func() {
		require.NoError(t, scheduler.Stop(t.Context()))
	}()

	// Advance the fake clock progressively past the next activation
	// (10:30:00 fake); the loop re-checks substituted clocks at least once
	// per sleep quantum.
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		clock.Advance(time.Second)

		select {
		case <-fired:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}

	t.Fatal("cron expression never fired under the fake clock")
}
