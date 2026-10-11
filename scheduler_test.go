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

// raiseMax atomically raises peak to at least current.
func raiseMax(peak *atomic.Int32, current int32) {
	for {
		seen := peak.Load()

		if current <= seen || peak.CompareAndSwap(seen, current) {
			return
		}
	}
}

// oneShotSchedule is a custom [hqgoscheduler.Schedule] that fires exactly
// once at a fixed instant: evaluations before the instant return it, later
// evaluations return the zero time, so the scheduler never activates the
// entry again.
type oneShotSchedule struct {
	at time.Time
}

// Next returns the one-shot activation time while it still lies ahead, and
// the zero [time.Time] afterwards.
//
// Parameters:
//   - after (time.Time): the reference instant (exclusive lower bound).
//
// Returns:
//   - next (time.Time): the activation time, or zero once it has passed.
func (s oneShotSchedule) Next(after time.Time) (next time.Time) {
	if after.Before(s.at) {
		next = s.at
	}

	return
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
	require.EqualError(t, err, "scheduler.Register: job is nil")
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

		raiseMax(&maxInFlight, current)

		time.Sleep(60 * time.Millisecond)

		inFlight.Add(-1)

		runs.Add(1)

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	// Let several activations pass while a run is in flight: each 60ms run
	// spans about six 10ms activations, all of which must be skipped.
	require.Eventually(t, func() (ok bool) {
		ok = runs.Load() >= 2

		return
	}, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))

	// Overlapping activations are skipped: never two in flight, and far fewer
	// completions than the activations a naive runner would have started.
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

			require.Contains(t, failure.Error(), "scheduler.runJobSafely:")
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

	// Every failed run is delivered through the panicking callback; two
	// deliveries prove the panic is recovered, and reaching Stop at all
	// proves the scheduling loop survived it.
	require.Eventually(t, func() (ok bool) {
		ok = runs.Load() >= 2

		return
	}, 3*time.Second, 5*time.Millisecond)

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
		runs        atomic.Int32
		inFlight    atomic.Int32
		maxInFlight atomic.Int32
	)

	work := hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		runs.Add(1)

		current := inFlight.Add(1)

		raiseMax(&maxInFlight, current)

		time.Sleep(40 * time.Millisecond)

		inFlight.Add(-1)

		return
	})

	_, err := scheduler.Register("@every 10ms", hqgoscheduler.NewJob("one", work))
	require.NoError(t, err)

	_, err = scheduler.Register("@every 10ms", hqgoscheduler.NewJob("two", work))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	// Let several runs complete while the semaphore serializes them.
	require.Eventually(t, func() (ok bool) {
		ok = runs.Load() >= 4

		return
	}, 3*time.Second, 5*time.Millisecond)

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

	// The loop exits immediately on a cancelled lifecycle context: give it a
	// moment to prove nothing fires, then shut down cleanly.
	require.Never(t, func() (fired bool) {
		fired = runs.Load() > 0

		return
	}, 50*time.Millisecond, 5*time.Millisecond)
	require.Zero(t, runs.Load())

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

func TestScheduler_RegisterSchedule(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, time.January, 15, 10, 30, 0, 0, time.UTC)

	clock := newFakeClock(start)

	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithClock(clock.Now),
	)

	schedule := oneShotSchedule{at: start.Add(time.Minute)}

	fired := make(chan struct{}, 1)

	id, err := scheduler.RegisterSchedule(schedule, hqgoscheduler.NewJob("once", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		select {
		case fired <- struct{}{}:
		default:
		}

		return
	})))
	require.NoError(t, err)
	require.Positive(t, id)

	scheduler.Start(t.Context())

	defer func() {
		require.NoError(t, scheduler.Stop(t.Context()))
	}()

	// Advance the fake clock until the one-shot activation fires; the loop
	// re-checks substituted clocks at least once per sleep quantum.
	require.Eventually(t, func() (ok bool) {
		clock.Advance(time.Second)

		select {
		case <-fired:
			ok = true
		default:
		}

		return
	}, 5*time.Second, 5*time.Millisecond)

	// The custom schedule is exposed through Entries.
	entries := scheduler.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, schedule, entries[0].Schedule)
	require.Equal(t, schedule.at, entries[0].Previous)

	// One-shot: advancing well past the activation must not fire again.
	for range 10 {
		clock.Advance(time.Minute)
	}

	require.Never(t, func() (firedAgain bool) {
		select {
		case <-fired:
			firedAgain = true
		default:
		}

		return
	}, 200*time.Millisecond, 5*time.Millisecond)
}

func TestScheduler_RegisterScheduleValidation(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	job := hqgoscheduler.NewJob("job", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	}))

	_, err := scheduler.RegisterSchedule(nil, job)
	require.EqualError(t, err, "scheduler.RegisterSchedule: schedule is nil")

	_, err = scheduler.RegisterSchedule(hqgoscheduler.IntervalSchedule{Every: time.Minute}, nil)
	require.EqualError(t, err, "scheduler.Register: job is nil")
}

func TestScheduler_StopSkipsQueuedRun(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, time.January, 15, 10, 30, 0, 0, time.UTC)

	clock := newFakeClock(start)

	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithClock(clock.Now),
		hqgoscheduler.WithMaxConcurrency(1),
	)

	var aRuns atomic.Int32

	aStarted := make(chan struct{})

	// Job A holds the single concurrency slot until shutdown cancels its
	// context.
	_, err := scheduler.Register("@every 1m", hqgoscheduler.NewJob("blocker", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		aRuns.Add(1)

		select {
		case <-aStarted:
		default:
			close(aStarted)
		}

		<-ctx.Done()

		err = ctx.Err()

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	// Advance the fake clock past A's activation and wait until it is
	// running, holding the slot.
	require.Eventually(t, func() (ok bool) {
		clock.Advance(time.Second)

		select {
		case <-aStarted:
			ok = true
		default:
		}

		return
	}, 5*time.Second, 5*time.Millisecond)

	// Only now register job B: it fires while A still holds the slot and
	// queues on the semaphore behind it.
	var bRuns atomic.Int32

	_, err = scheduler.Register("@every 1m", hqgoscheduler.NewJob("queued", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		bRuns.Add(1)

		return
	})))
	require.NoError(t, err)

	// Advance past B's activation and wait until it has fired — recorded by
	// its previous-activation timestamp — and is queued behind A.
	require.Eventually(t, func() (ok bool) {
		clock.Advance(10 * time.Second)

		entries := scheduler.Entries()
		if len(entries) != 2 {
			return
		}

		ok = !entries[1].Previous.IsZero()

		return
	}, 5*time.Second, 5*time.Millisecond)

	// A honors cancellation, so Stop completes; B, queued behind A, must
	// never execute.
	require.NoError(t, scheduler.Stop(t.Context()))

	require.Equal(t, int32(1), aRuns.Load())
	require.Zero(t, bRuns.Load())
}

func TestScheduler_UnregisterWhileInFlight(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, time.January, 15, 10, 30, 0, 0, time.UTC)

	clock := newFakeClock(start)

	var (
		mu     sync.Mutex
		events []hqgoscheduler.ErrorEvent
	)

	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithClock(clock.Now),
		hqgoscheduler.WithOnError(func(ev hqgoscheduler.ErrorEvent) {
			mu.Lock()

			defer mu.Unlock()

			events = append(events, ev)
		}),
	)

	started := make(chan struct{})
	release := make(chan struct{})
	boom := errors.New("boom")

	id, err := scheduler.Register("@every 1m", hqgoscheduler.NewJob("in-flight", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		select {
		case <-started:
		default:
			close(started)
		}

		<-release

		err = boom

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	// Advance the fake clock so the run starts, then unregister the entry
	// while it is in flight: the run is not interrupted.
	require.Eventually(t, func() (ok bool) {
		clock.Advance(time.Second)

		select {
		case <-started:
			ok = true
		default:
		}

		return
	}, 5*time.Second, 5*time.Millisecond)

	scheduler.Unregister(id)

	require.Empty(t, scheduler.Entries())

	close(release)

	// The failed run is still reported; its entry was removed while in
	// flight, so the snapshot carries only the identifier and the job.
	require.Eventually(t, func() (ok bool) {
		mu.Lock()

		defer mu.Unlock()

		if len(events) != 1 {
			return
		}

		ok = errors.Is(events[0].Err, boom) &&
			events[0].Entry.ID == id &&
			events[0].Entry.Job != nil &&
			events[0].Entry.Schedule == nil

		return
	}, 5*time.Second, 5*time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))
}

func TestScheduler_EntriesReturnsIndependentSnapshot(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	job := hqgoscheduler.NewJob("job", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	}))

	id1, err := scheduler.Register("@every 1m", job)
	require.NoError(t, err)

	id2, err := scheduler.Register("@every 2m", job)
	require.NoError(t, err)

	first := scheduler.Entries()
	require.Len(t, first, 2)

	// Mutate the snapshot aggressively: the scheduler must be unaffected.
	first[0], first[1] = first[1], first[0]
	first[0].ID = -1
	first[0].Job = nil
	first[0].Schedule = nil
	first[1].Next = time.Date(1999, time.January, 1, 0, 0, 0, 0, time.UTC)

	second := scheduler.Entries()
	require.Len(t, second, 2)
	require.Equal(t, id1, second[0].ID)
	require.Equal(t, id2, second[1].ID)
	require.NotNil(t, second[0].Schedule)
	require.NotNil(t, second[0].Job)
	require.NotNil(t, second[1].Schedule)
	require.NotNil(t, second[1].Job)
}
