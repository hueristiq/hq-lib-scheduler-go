//nolint:testpackage // tests exercise unexported behavior.
package scheduler

import (
	"testing"
	"time"
)

// specNextSink keeps benchmark results alive so the compiler cannot optimise
// the measured calls away.
var specNextSink time.Time

// BenchmarkSpecScheduleNext measures [specSchedule.Next] across representative
// expression shapes: the dense wildcard (minute-stepping), a typical sparse
// expression (skip ladder), a rare leap-day match, and an impossible
// expression that scans the full search window before giving up.
func BenchmarkSpecScheduleNext(b *testing.B) {
	after := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		spec string
	}{
		{name: "every_minute", spec: "* * * * *"},
		{name: "weekday_morning", spec: "30 9 * * MON-FRI"},
		{name: "leap_day", spec: "0 0 29 2 *"},
		{name: "impossible", spec: "0 0 31 2 *"},
	}

	for _, tc := range cases {
		schedule, err := Parse(tc.spec)
		if err != nil {
			b.Fatalf("parsing %q: %v", tc.spec, err)
		}

		parsed, ok := schedule.(*specSchedule)
		if !ok {
			b.Fatalf("spec %q did not parse to a *specSchedule", tc.spec)
		}

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			var next time.Time

			for range b.N {
				next = parsed.Next(after)
			}

			specNextSink = next
		})
	}
}
