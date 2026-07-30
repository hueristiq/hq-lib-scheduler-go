package scheduler_test

import (
	"context"
	"errors"
	"strings"
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

func TestScheduler_AddAndEntries(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Date(2025, time.January, 15, 10, 30, 0, 0, time.UTC))

	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithClock(clock.Now),
	)

	job := hqgoscheduler.NewJob("heartbeat", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	}))

	id1, err := scheduler.Add("0 12 * * *", job)
	require.NoError(t, err)
	require.Positive(t, id1)

	id2, err := scheduler.AddEvery(time.Minute, job)
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

	scheduler.Remove(id1)

	entries = scheduler.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, id2, entries[0].ID)

	// Removing an unknown identifier is a no-op.
	scheduler.Remove(9999)
	require.Len(t, scheduler.Entries(), 1)
}

func TestScheduler_AddValidation(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	job := hqgoscheduler.NewJob("job", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	}))

	_, err := scheduler.Add("not a spec", job)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scheduler.Add")

	_, err = scheduler.AddEvery(0, job)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scheduler.AddEvery")

	_, err = scheduler.AddEvery(-time.Second, job)
	require.Error(t, err)

	_, err = scheduler.AddEvery(time.Second, nil)
	require.Error(t, err)

	_, err = scheduler.AddSchedule(nil, job)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scheduler.AddSchedule")

	_, err = scheduler.AddSchedule(hqgoscheduler.IntervalSchedule{Every: time.Second}, nil)
	require.Error(t, err)
}

func TestScheduler_AddSchedule(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	var runs atomic.Int32

	_, err := scheduler.AddSchedule(
		hqgoscheduler.IntervalSchedule{Every: 20 * time.Millisecond},
		hqgoscheduler.NewJob("custom", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
			runs.Add(1)

			return
		})),
	)
	require.NoError(t, err)

	scheduler.Start(t.Context())

	require.Eventually(t, func() (ok bool) {
		ok = runs.Load() >= 2

		return
	}, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))
}

func TestScheduler_RunsJobs(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	var runs atomic.Int32

	_, err := scheduler.AddEvery(20*time.Millisecond, hqgoscheduler.NewJob("counter", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
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

	_, err := scheduler.AddEvery(10*time.Millisecond, hqgoscheduler.NewJob("slow", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
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

func TestScheduler_ErrorHandler(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		failures []error
		entries  []hqgoscheduler.Entry
	)

	scheduler := hqgoscheduler.NewScheduler(hqgoscheduler.WithErrorHandler(func(entry hqgoscheduler.Entry, err error) {
		mu.Lock()

		defer mu.Unlock()

		failures = append(failures, err)
		entries = append(entries, entry)
	}))

	boom := errors.New("boom")

	_, err := scheduler.AddEvery(15*time.Millisecond, hqgoscheduler.NewJob("failing", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		err = boom

		return
	})))
	require.NoError(t, err)

	_, err = scheduler.AddEvery(15*time.Millisecond, hqgoscheduler.NewJob("panicking", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
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

func TestScheduler_StopCancelsPerRunContext(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler()

	started := make(chan struct{})

	_, err := scheduler.AddEvery(10*time.Millisecond, hqgoscheduler.NewJob("blocking", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
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

	_, err := scheduler.AddEvery(10*time.Millisecond, hqgoscheduler.NewJob("stubborn", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
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

	_, err := scheduler.AddEvery(10*time.Millisecond, hqgoscheduler.NewJob("one", work))
	require.NoError(t, err)

	_, err = scheduler.AddEvery(10*time.Millisecond, hqgoscheduler.NewJob("two", work))
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

	_, err := scheduler.AddEvery(15*time.Millisecond, hqgoscheduler.NewJob("counter", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
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

	_, err := scheduler.AddEvery(10*time.Millisecond, hqgoscheduler.NewJob("never", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
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

	_, err := scheduler.Add("*/1 * * * *", hqgoscheduler.NewJob("minutely", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
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

// logRecord captures one record emitted to a recordingLogger.
//
// Fields:
//   - level (string): the level the record was logged at.
//   - msg (string): the log message.
//   - args ([]any): the alternating key-value pairs annotating the record.
type logRecord struct {
	level string
	msg   string
	args  []any
}

// arg returns the value paired with key in the record's alternating
// key-value args.
func (r logRecord) arg(key string) (value any, ok bool) {
	for i := 0; i+1 < len(r.args); i += 2 {
		if r.args[i] == key {
			value, ok = r.args[i+1], true

			return
		}
	}

	return
}

// recordingLogger is a mutex-guarded logger fake capturing every record it
// receives, for use through [hqgoscheduler.WithLogger].
type recordingLogger struct {
	mu      sync.Mutex
	records []logRecord
}

// Debug records a debug record.
func (l *recordingLogger) Debug(msg string, args ...any) {
	l.record("debug", msg, args)
}

// Info records an info record.
func (l *recordingLogger) Info(msg string, args ...any) {
	l.record("info", msg, args)
}

// Warn records a warning record.
func (l *recordingLogger) Warn(msg string, args ...any) {
	l.record("warn", msg, args)
}

// Error records an error record.
func (l *recordingLogger) Error(msg string, args ...any) {
	l.record("error", msg, args)
}

// record appends one record under the lock.
func (l *recordingLogger) record(level, msg string, args []any) {
	l.mu.Lock()

	defer l.mu.Unlock()

	l.records = append(l.records, logRecord{level: level, msg: msg, args: args})
}

// has reports whether a record at level whose message contains msgSubstring
// was captured.
func (l *recordingLogger) has(level, msgSubstring string) (ok bool) {
	_, ok = l.find(level, msgSubstring)

	return
}

// find returns the first record at level whose message contains
// msgSubstring, for attribute inspection.
func (l *recordingLogger) find(level, msgSubstring string) (record logRecord, ok bool) {
	l.mu.Lock()

	defer l.mu.Unlock()

	for _, captured := range l.records {
		if captured.level == level && strings.Contains(captured.msg, msgSubstring) {
			record, ok = captured, true

			return
		}
	}

	return
}

func TestScheduler_LoggerSuccess(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Date(2025, time.January, 15, 10, 30, 0, 0, time.UTC))

	recorder := &recordingLogger{}

	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithClock(clock.Now),
		hqgoscheduler.WithLogger(recorder),
	)

	_, err := scheduler.AddEvery(time.Minute, hqgoscheduler.NewJob("report", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	defer func() {
		require.NoError(t, scheduler.Stop(t.Context()))
	}()

	// Advance the fake clock past the first activation; the loop re-checks
	// substituted clocks at least once per sleep quantum.
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) && !recorder.has("debug", "job run finished") {
		clock.Advance(time.Minute)

		time.Sleep(20 * time.Millisecond)
	}

	require.True(t, recorder.has("debug", "job run started"))
	require.True(t, recorder.has("debug", "job run finished"))

	record, ok := recorder.find("debug", "job run finished")
	require.True(t, ok)

	_, ok = record.arg("duration")
	require.True(t, ok)
}

func TestScheduler_LoggerFailure(t *testing.T) {
	t.Parallel()

	recorder := &recordingLogger{}

	handlerCalled := make(chan struct{}, 1)

	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLogger(recorder),
		hqgoscheduler.WithErrorHandler(func(entry hqgoscheduler.Entry, err error) {
			select {
			case handlerCalled <- struct{}{}:
			default:
			}
		}),
	)

	boom := errors.New("boom")

	_, err := scheduler.AddEvery(15*time.Millisecond, hqgoscheduler.NewJob("failing", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		err = boom

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	defer func() {
		require.NoError(t, scheduler.Stop(t.Context()))
	}()

	require.Eventually(t, func() (ok bool) {
		ok = recorder.has("error", "job run failed")

		return
	}, 3*time.Second, 5*time.Millisecond)

	record, ok := recorder.find("error", "job run failed")
	require.True(t, ok)

	name, ok := record.arg("job")
	require.True(t, ok)
	require.Equal(t, "failing", name)

	failure, ok := record.arg("error")
	require.True(t, ok)
	require.Equal(t, boom, failure)

	// Logging never replaces the error handler.
	select {
	case <-handlerCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("error handler never invoked")
	}
}

func TestScheduler_LoggerPanic(t *testing.T) {
	t.Parallel()

	recorder := &recordingLogger{}

	scheduler := hqgoscheduler.NewScheduler(hqgoscheduler.WithLogger(recorder))

	_, err := scheduler.AddEvery(15*time.Millisecond, hqgoscheduler.NewJob("panicking", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		panic("kaput")
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	defer func() {
		require.NoError(t, scheduler.Stop(t.Context()))
	}()

	require.Eventually(t, func() (ok bool) {
		record, found := recorder.find("error", "job run failed")
		if !found {
			return
		}

		value, found := record.arg("error")
		if !found {
			return
		}

		runErr, found := value.(error)

		ok = found && strings.Contains(runErr.Error(), "panicked")

		return
	}, 3*time.Second, 5*time.Millisecond)
}

func TestScheduler_LoggerSkip(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Date(2025, time.January, 15, 10, 30, 0, 0, time.UTC))

	recorder := &recordingLogger{}

	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithClock(clock.Now),
		hqgoscheduler.WithLogger(recorder),
	)

	release := make(chan struct{})

	_, err := scheduler.AddEvery(time.Minute, hqgoscheduler.NewJob("blocking", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		select {
		case <-release:
		case <-ctx.Done():
		}

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	defer func() {
		close(release)

		require.NoError(t, scheduler.Stop(t.Context()))
	}()

	// Advance the fake clock past several activations while the first run is
	// still in flight; every later activation must be skipped.
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) && !recorder.has("info", "skipped") {
		clock.Advance(time.Minute)

		time.Sleep(20 * time.Millisecond)
	}

	require.True(t, recorder.has("info", "skipped"))

	record, ok := recorder.find("info", "skipped")
	require.True(t, ok)

	_, ok = record.arg("scheduled")
	require.True(t, ok)
}

func TestScheduler_LoggerLifecycle(t *testing.T) {
	t.Parallel()

	recorder := &recordingLogger{}

	scheduler := hqgoscheduler.NewScheduler(hqgoscheduler.WithLogger(recorder))

	job := hqgoscheduler.NewJob("heartbeat", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	}))

	_, err := scheduler.AddEvery(time.Hour, job)
	require.NoError(t, err)

	scheduler.Start(t.Context())

	require.NoError(t, scheduler.Stop(t.Context()))

	require.True(t, recorder.has("debug", "entry added"))
	require.True(t, recorder.has("debug", "scheduler started"))
	require.True(t, recorder.has("debug", "scheduler stopped"))
}

func TestScheduler_LoggerNilIgnored(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler(hqgoscheduler.WithLogger(nil))

	var runs atomic.Int32

	_, err := scheduler.AddEvery(15*time.Millisecond, hqgoscheduler.NewJob("counter", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		runs.Add(1)

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	require.Eventually(t, func() (ok bool) {
		ok = runs.Load() >= 1

		return
	}, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, scheduler.Stop(t.Context()))
}

// panickingLogger is a logger fake whose every method panics, to verify the
// scheduler recovers logger panics instead of crashing.
type panickingLogger struct{}

// Debug panics.
func (panickingLogger) Debug(msg string, args ...any) { panic("boom") }

// Info panics.
func (panickingLogger) Info(msg string, args ...any) { panic("boom") }

// Warn panics.
func (panickingLogger) Warn(msg string, args ...any) { panic("boom") }

// Error panics.
func (panickingLogger) Error(msg string, args ...any) { panic("boom") }

func TestScheduler_LoggerPanicRecovered(t *testing.T) {
	t.Parallel()

	scheduler := hqgoscheduler.NewScheduler(hqgoscheduler.WithLogger(panickingLogger{}))

	var runs atomic.Int32

	entryID, err := scheduler.AddEvery(15*time.Millisecond, hqgoscheduler.NewJob("counter", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		runs.Add(1)

		return
	})))
	require.NoError(t, err)

	scheduler.Start(t.Context())

	require.Eventually(t, func() (ok bool) {
		ok = runs.Load() >= 1

		return
	}, 3*time.Second, 5*time.Millisecond)

	scheduler.Remove(entryID)

	require.NoError(t, scheduler.Stop(t.Context()))
}
