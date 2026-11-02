package scheduler

import (
	"time"
)

// Schedule describes a set of activation times, answering a single question:
// given an instant, when is the next activation strictly after it.
//
// Implementations must be safe for concurrent use: a single Schedule may be
// evaluated from the scheduling loop while callers hold it.
type Schedule interface {
	// Next returns the next activation time strictly after after, or the zero
	// [time.Time] when no future activation exists.
	//
	// Parameters:
	//   - after (time.Time): the reference instant (exclusive lower bound).
	//
	// Returns:
	//   - next (time.Time): the next activation time, or zero when none exists.
	Next(after time.Time) (next time.Time)
}

// IntervalSchedule is a [Schedule] that fires at a fixed interval measured
// from the moment it is first evaluated.
//
// Fields:
//   - Every (time.Duration): fixed interval between activations. It must be
//     strictly positive; a non-positive interval never activates.
type IntervalSchedule struct {
	Every time.Duration
}

// Next returns the next activation time of the [IntervalSchedule], computed
// as after plus [IntervalSchedule.Every]. A non-positive interval yields the
// zero [time.Time], so a misconfigured schedule never spins the scheduling
// loop.
//
// Parameters:
//   - after (time.Time): the reference instant (exclusive lower bound).
//
// Returns:
//   - next (time.Time): after plus the configured interval, or zero when the
//     interval is non-positive.
func (s IntervalSchedule) Next(after time.Time) (next time.Time) {
	if s.Every <= 0 {
		return
	}

	next = after.Add(s.Every)

	return
}

// Compile-time guard ensuring [IntervalSchedule] satisfies [Schedule].
var _ Schedule = IntervalSchedule{}
