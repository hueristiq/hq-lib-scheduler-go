package scheduler_test

import (
	"testing"
	"time"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

// TestIntervalSchedule covers the fixed-interval schedule.
func TestIntervalSchedule(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name  string
		every time.Duration
		want  time.Time
	}{
		{name: "positive interval", every: time.Minute, want: now.Add(time.Minute)},
		{name: "zero interval never fires", every: 0, want: time.Time{}},
		{name: "negative interval never fires", every: -time.Minute, want: time.Time{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schedule := hqgoscheduler.IntervalSchedule{Every: tt.every}

			if got := schedule.Next(now); !got.Equal(tt.want) {
				t.Errorf("Next() = %v; want %v", got, tt.want)
			}
		})
	}
}
