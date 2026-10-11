package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

// at builds a UTC instant for golden next-fire expectations.
func at(year int, month time.Month, day, hour, minute int) (t time.Time) {
	t = time.Date(year, month, day, hour, minute, 0, 0, time.UTC)

	return
}

func TestParse_GoldenNextFire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		spec  string
		after time.Time
		want  time.Time
	}{
		{
			name:  "wildcard every minute",
			spec:  "* * * * *",
			after: at(2025, time.January, 15, 10, 30),
			want:  at(2025, time.January, 15, 10, 31),
		},
		{
			name:  "wildcard rounds up from mid-minute",
			spec:  "* * * * *",
			after: at(2025, time.January, 15, 10, 30).Add(30 * time.Second),
			want:  at(2025, time.January, 15, 10, 31),
		},
		{
			name:  "fixed time daily",
			spec:  "30 9 * * *",
			after: at(2025, time.January, 15, 10, 30),
			want:  at(2025, time.January, 16, 9, 30),
		},
		{
			name:  "step every 15 minutes",
			spec:  "*/15 * * * *",
			after: at(2025, time.January, 15, 10, 7),
			want:  at(2025, time.January, 15, 10, 15),
		},
		{
			name:  "hour range with step on weekdays",
			spec:  "0 9-17/2 * * MON-FRI",
			after: at(2025, time.January, 15, 10, 0), // Wednesday
			want:  at(2025, time.January, 15, 11, 0),
		},
		{
			// The reference instant matches exactly, but Next is strictly
			// after, so the activation moves to the next weekday.
			name:  "weekday range skips weekend",
			spec:  "0 9 * * MON-FRI",
			after: at(2025, time.January, 17, 9, 0), // Friday 09:00
			want:  at(2025, time.January, 20, 9, 0), // Monday
		},
		{
			name:  "comma list of months and days",
			spec:  "0 0 1 JAN,JUL *",
			after: at(2025, time.February, 10, 0, 0),
			want:  at(2025, time.July, 1, 0, 0),
		},
		{
			name:  "case-insensitive names",
			spec:  "0 0 * jan SuN",
			after: at(2025, time.January, 4, 0, 0), // Saturday
			want:  at(2025, time.January, 5, 0, 0), // Sunday
		},
		{
			name:  "question mark aliases wildcard",
			spec:  "0 12 ? * *",
			after: at(2025, time.January, 15, 13, 0),
			want:  at(2025, time.January, 16, 12, 0),
		},
		{
			name:  "day-of-month and day-of-week use OR semantics",
			spec:  "0 0 13 * FRI",
			after: at(2025, time.June, 12, 0, 0), // Thursday June 12
			want:  at(2025, time.June, 13, 0, 0), // Friday the 13th
		},
		{
			name:  "OR semantics fire on Friday without the 13th",
			spec:  "0 0 13 * FRI",
			after: at(2025, time.June, 14, 0, 0), // Saturday June 14
			want:  at(2025, time.June, 20, 0, 0), // next Friday
		},
		{
			name:  "day-of-week 7 is Sunday",
			spec:  "0 9 * * 7",
			after: at(2025, time.January, 15, 10, 0), // Wednesday
			want:  at(2025, time.January, 19, 9, 0),  // Sunday
		},
		{
			name:  "day-of-week range including 7",
			spec:  "0 9 * * 5-7",
			after: at(2025, time.January, 15, 10, 0), // Wednesday
			want:  at(2025, time.January, 17, 9, 0),  // Friday
		},
		{
			name:  "leap day",
			spec:  "0 0 29 2 *",
			after: at(2023, time.March, 1, 0, 0),
			want:  at(2024, time.February, 29, 0, 0),
		},
		{
			// 2100 is not a leap year: the gap 2096 -> 2104 is the longest
			// possible between leap days, and it must not exhaust the search
			// window.
			name:  "leap day across a non-leap century year",
			spec:  "0 0 29 2 *",
			after: at(2097, time.March, 1, 0, 0),
			want:  at(2104, time.February, 29, 0, 0),
		},
		{
			name:  "year boundary",
			spec:  "0 0 1 1 *",
			after: at(2025, time.June, 15, 12, 0),
			want:  at(2026, time.January, 1, 0, 0),
		},
		{
			name:  "descriptor daily",
			spec:  "@daily",
			after: at(2025, time.January, 15, 10, 30),
			want:  at(2025, time.January, 16, 0, 0),
		},
		{
			name:  "descriptor midnight",
			spec:  "@midnight",
			after: at(2025, time.January, 15, 10, 30),
			want:  at(2025, time.January, 16, 0, 0),
		},
		{
			name:  "descriptor hourly",
			spec:  "@hourly",
			after: at(2025, time.January, 15, 10, 30),
			want:  at(2025, time.January, 15, 11, 0),
		},
		{
			name:  "descriptor monthly",
			spec:  "@monthly",
			after: at(2025, time.January, 15, 10, 30),
			want:  at(2025, time.February, 1, 0, 0),
		},
		{
			name:  "descriptor weekly starts Sunday",
			spec:  "@weekly",
			after: at(2025, time.January, 15, 10, 30), // Wednesday
			want:  at(2025, time.January, 19, 0, 0),   // Sunday
		},
		{
			name:  "descriptor yearly and annually agree",
			spec:  "@annually",
			after: at(2025, time.January, 1, 0, 0),
			want:  at(2026, time.January, 1, 0, 0),
		},
		{
			name:  "bare value with step means value-to-max",
			spec:  "5/20 * * * *",
			after: at(2025, time.January, 15, 10, 6),
			want:  at(2025, time.January, 15, 10, 25),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schedule, err := hqgoscheduler.Parse(tt.spec)
			require.NoError(t, err)

			next := schedule.Next(tt.after)
			require.Equal(t, tt.want, next)
		})
	}
}

func TestParse_ImpossibleScheduleYieldsZeroTime(t *testing.T) {
	t.Parallel()

	schedule, err := hqgoscheduler.Parse("0 0 31 2 *")
	require.NoError(t, err)

	next := schedule.Next(at(2025, time.January, 1, 0, 0))
	require.True(t, next.IsZero())
}

func TestParse_EveryInterval(t *testing.T) {
	t.Parallel()

	schedule, err := hqgoscheduler.Parse("@every 30s")
	require.NoError(t, err)

	interval, ok := schedule.(hqgoscheduler.IntervalSchedule)
	require.True(t, ok)
	require.Equal(t, 30*time.Second, interval.Every)

	after := at(2025, time.January, 15, 10, 30)
	require.Equal(t, after.Add(30*time.Second), schedule.Next(after))

	schedule, err = hqgoscheduler.Parse("@every 1h30m")
	require.NoError(t, err)
	require.Equal(t, after.Add(90*time.Minute), schedule.Next(after))
}

func TestIntervalSchedule_NonPositiveIntervalYieldsZeroTime(t *testing.T) {
	t.Parallel()

	after := at(2025, time.January, 15, 10, 30)

	require.True(t, hqgoscheduler.IntervalSchedule{}.Next(after).IsZero())
	require.True(t, hqgoscheduler.IntervalSchedule{Every: -time.Second}.Next(after).IsZero())
}

func TestParse_InvalidSpecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec string
	}{
		{name: "empty", spec: ""},
		{name: "whitespace only", spec: "   "},
		{name: "too few fields", spec: "* * * *"},
		{name: "too many fields", spec: "* * * * * *"},
		{name: "unknown descriptor", spec: "@fortnightly"},
		{name: "descriptor resembling every", spec: "@everyother 5m"},
		{name: "every without duration", spec: "@every"},
		{name: "every with extra fields", spec: "@every 5m extra"},
		{name: "every with invalid duration", spec: "@every abc"},
		{name: "every with zero duration", spec: "@every 0s"},
		{name: "every with negative duration", spec: "@every -5s"},
		{name: "minute out of range", spec: "61 * * * *"},
		{name: "hour out of range", spec: "* 24 * * *"},
		{name: "day-of-month out of range", spec: "* * 0 * *"},
		{name: "month out of range", spec: "* * * 13 *"},
		{name: "day-of-week out of range", spec: "* * * * 8"},
		{name: "inverted range", spec: "10-5 * * * *"},
		{name: "malformed range", spec: "1-2-3 * * * *"},
		{name: "malformed step", spec: "*/0 * * * *"},
		{name: "negative step", spec: "*/-2 * * * *"},
		{name: "double slash", spec: "*/*/* * * * *"},
		{name: "non-numeric value", spec: "x * * * *"},
		{name: "signed value", spec: "+5 * * * *"},
		{name: "unknown name in wrong field", spec: "* * * * JAN"},
		{name: "empty list item", spec: "1,,2 * * * *"},
		{name: "question mark in minute field", spec: "? * * * *"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schedule, err := hqgoscheduler.Parse(tt.spec)
			require.Error(t, err)
			require.Nil(t, schedule)
			require.Contains(t, err.Error(), "scheduler.Parse")
		})
	}
}

func TestParse_DSTTransitions(t *testing.T) {
	t.Parallel()

	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	tests := []struct {
		name  string
		spec  string
		after time.Time
		want  time.Time
	}{
		{
			// 2025-03-09 02:00 EST does not exist (spring forward): the
			// activation moves past the gap to the next day.
			name:  "spring forward skips the nonexistent hour",
			spec:  "0 2 * * *",
			after: time.Date(2025, time.March, 9, 1, 30, 0, 0, loc),
			want:  time.Date(2025, time.March, 10, 2, 0, 0, 0, loc),
		},
		{
			name:  "spring forward skips a minute inside the gap",
			spec:  "30 2 * * *",
			after: time.Date(2025, time.March, 9, 1, 0, 0, 0, loc),
			want:  time.Date(2025, time.March, 10, 2, 30, 0, 0, loc),
		},
		{
			name:  "spring forward minute walk jumps the gap",
			spec:  "* * * * *",
			after: time.Date(2025, time.March, 9, 1, 59, 0, 0, loc),
			want:  time.Date(2025, time.March, 9, 3, 0, 0, 0, loc),
		},
		{
			// 2025-11-02 01:00 occurs twice (fall back): the first
			// occurrence activates.
			name:  "fall back first occurrence",
			spec:  "0 1 * * *",
			after: time.Date(2025, time.November, 2, 0, 30, 0, 0, loc),
			want:  time.Date(2025, time.November, 2, 1, 0, 0, 0, loc),
		},
		{
			// And the second occurrence follows: an evaluation inside the
			// repeated hour lands on it (01:00 EST, built deterministically
			// as 05:00 EST minus four hours).
			name:  "fall back second occurrence",
			spec:  "0 1 * * *",
			after: time.Date(2025, time.November, 2, 1, 0, 0, 0, loc),
			want:  time.Date(2025, time.November, 2, 5, 0, 0, 0, loc).Add(-4 * time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schedule, err := hqgoscheduler.Parse(tt.spec)
			require.NoError(t, err)

			next := schedule.Next(tt.after)
			require.Equal(t, tt.want, next)
		})
	}
}
