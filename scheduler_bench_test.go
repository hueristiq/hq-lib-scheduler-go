package scheduler_test

import (
	"context"
	"testing"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

// BenchmarkSchedulerFireAmongMany measures the scheduling loop's per-wake
// cost when one hot entry fires among many idle ones: each activation of the
// hot entry requires one loop iteration, so the benchmark isolates the
// wake-path cost the min-heap restructure targets.
func BenchmarkSchedulerFireAmongMany(b *testing.B) {
	scheduler := hqgoscheduler.NewScheduler()

	idle := hqgoscheduler.NewJob("idle", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	}))

	for range 50000 {
		if _, err := scheduler.Register("@every 1h", idle); err != nil {
			b.Fatalf("registering idle entry: %v", err)
		}
	}

	fired := make(chan struct{}, 1)

	_, err := scheduler.Register("@every 1ms", hqgoscheduler.NewJob("hot", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		select {
		case fired <- struct{}{}:
		default:
		}

		return
	})))
	if err != nil {
		b.Fatalf("registering hot entry: %v", err)
	}

	scheduler.Start(b.Context())

	b.ResetTimer()

	for range b.N {
		<-fired
	}

	b.StopTimer()

	if err := scheduler.Stop(b.Context()); err != nil {
		b.Fatalf("stopping scheduler: %v", err)
	}
}
