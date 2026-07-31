package scheduler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// searchWindowYears bounds how far into the future [specSchedule.Next] scans
// before giving up and returning the zero [time.Time]. Eight years covers the
// longest possible gap between leap days — 2096-02-29 to 2104-02-29 across a
// non-leap century year — so every satisfiable expression matches within the
// window, and only truly impossible ones (such as February 31st) scan it in
// full.
const searchWindowYears = 8

// descriptors maps the `@`-prefixed shorthand names to their equivalent
// standard 5-field expressions.
var descriptors = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// monthNames maps case-insensitive three-letter month names to their numeric
// value (1-12).
var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

// dowNames maps case-insensitive three-letter day-of-week names to their
// numeric value (Sunday = 0).
var dowNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// specSchedule is the parsed form of a standard 5-field cron expression. Each
// field is a set of matching values encoded as a bitmask.
//
// Fields:
//   - minutes (uint64): matching minutes (0-59), one bit per value.
//   - hours (uint64): matching hours (0-23), one bit per value.
//   - dom (uint32): matching days of month (1-31), one bit per value.
//   - months (uint16): matching months (1-12), one bit per value.
//   - dow (uint8): matching days of week (0-6, Sunday = 0), one bit per value.
//   - domRestricted (bool): whether the day-of-month field was restricted
//     (not `*`/`?`), controlling the dom/dow OR-semantics.
//   - dowRestricted (bool): whether the day-of-week field was restricted.
type specSchedule struct {
	minutes       uint64
	hours         uint64
	dom           uint32
	months        uint16
	dow           uint8
	domRestricted bool
	dowRestricted bool
}

// Next returns the next activation time of the parsed cron expression,
// strictly after after, honoring the cron dom/dow OR-semantics: when both
// day-of-month and day-of-week are restricted, a day matches if either field
// matches.
//
// The search walks a skip ladder instead of scanning minute by minute: a
// non-matching month skips to the first of the next month, a non-matching day
// to the next midnight, a non-matching hour to the top of the next hour, and
// only a matching hour with a non-matching minute advances a single minute.
// Skips use civil-time arithmetic, so they stay correct across daylight-saving
// transitions.
//
// Parameters:
//   - after (time.Time): the reference instant (exclusive lower bound).
//
// Returns:
//   - next (time.Time): the next matching instant, truncated to the minute,
//     or the zero [time.Time] when no activation exists within the search
//     window (see [searchWindowYears]).
func (s *specSchedule) Next(after time.Time) (next time.Time) {
	// Cron fields are minute-granular: start from the first whole minute
	// strictly after the reference instant.
	candidate := after.Truncate(time.Minute).Add(time.Minute)

	limit := candidate.AddDate(searchWindowYears, 0, 0)

	for candidate.Before(limit) {
		switch {
		case s.months&(uint16(1)<<uint(candidate.Month())) == 0:
			// Skip to the first second of the next month.
			candidate = time.Date(candidate.Year(), candidate.Month(), 1, 0, 0, 0, 0, candidate.Location()).AddDate(0, 1, 0)
		case !s.dayMatches(candidate):
			// Skip to midnight of the next day.
			candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(), 0, 0, 0, 0, candidate.Location()).AddDate(0, 0, 1)
		case s.hours&(uint64(1)<<uint(candidate.Hour())) == 0:
			// Skip to the top of the next hour; time.Date normalizes an
			// hour of 24 to midnight of the next day.
			candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(), candidate.Hour()+1, 0, 0, 0, candidate.Location())
		case s.minutes&(uint64(1)<<uint(candidate.Minute())) == 0:
			candidate = candidate.Add(time.Minute)
		default:
			next = candidate

			return
		}
	}

	return
}

// dayMatches reports whether the calendar date of candidate satisfies the
// day-of-month and day-of-week constraints, with cron OR-semantics between
// the two fields: when both are restricted, either match suffices; when only
// one is restricted, that one decides; when neither is, every day matches.
//
// Parameters:
//   - candidate (time.Time): the instant under test.
//
// Returns:
//   - matched (bool): true when the date fields match.
func (s *specSchedule) dayMatches(candidate time.Time) (matched bool) {
	domMatch := s.dom&(uint32(1)<<uint(candidate.Day())) != 0

	dowMatch := s.dow&(uint8(1)<<uint(candidate.Weekday())) != 0

	switch {
	case s.domRestricted && s.dowRestricted:
		matched = domMatch || dowMatch
	case s.domRestricted:
		matched = domMatch
	case s.dowRestricted:
		matched = dowMatch
	default:
		matched = true
	}

	return
}

// Parse parses spec into a [Schedule].
//
// The accepted forms are:
//
//  1. Standard 5-field expressions `minute hour day-of-month month
//     day-of-week`, supporting `*`, `*/n`, single values, ranges `a-b`,
//     stepped ranges `a-b/n`, bare values with a step `a/n` (meaning "from a
//     to the maximum, every n"), comma-separated lists, case-insensitive
//     month names (JAN-DEC) and day-of-week names (SUN-SAT), and `?` as an
//     alias for `*` in the day-of-month and day-of-week fields. Day-of-week
//     accepts both 0 and 7 for Sunday.
//  2. Descriptors: `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`,
//     `@midnight`, `@hourly`.
//  3. `@every <duration>` — a Go duration (e.g. `@every 30s`,
//     `@every 1h30m`) producing an [IntervalSchedule].
//
// Parameters:
//   - spec (string): the cron expression, descriptor, or `@every` interval.
//
// Returns:
//   - schedule (Schedule): the parsed schedule.
//   - err (error): non-nil when spec is empty, malformed, or out of range.
func Parse(spec string) (schedule Schedule, err error) {
	spec = strings.TrimSpace(spec)

	if spec == "" {
		err = errors.New("scheduler.Parse: spec is empty")

		return
	}

	if strings.HasPrefix(spec, "@") {
		schedule, err = parseDescriptor(spec)

		return
	}

	schedule, err = parseStandard(spec)

	return
}

// parseDescriptor parses an `@`-prefixed spec: either `@every <duration>` or
// a named descriptor.
//
// Parameters:
//   - spec (string): the descriptor (leading `@` included).
//
// Returns:
//   - schedule (Schedule): the parsed schedule.
//   - err (error): non-nil when the descriptor is unknown or the duration is
//     invalid or non-positive.
func parseDescriptor(spec string) (schedule Schedule, err error) {
	fields := strings.Fields(spec)

	if len(fields) > 0 && strings.EqualFold(fields[0], "@every") {
		schedule, err = parseEvery(fields)

		return
	}

	expression, ok := descriptors[strings.ToLower(spec)]
	if !ok {
		err = fmt.Errorf("scheduler.Parse: unrecognized descriptor %q", spec)

		return
	}

	schedule, err = parseStandard(expression)

	return
}

// parseEvery parses the fields of an `@every <duration>` spec into an
// [IntervalSchedule].
//
// Parameters:
//   - fields ([]string): the whitespace-separated fields of the spec,
//     starting with `@every`.
//
// Returns:
//   - schedule (Schedule): the parsed [IntervalSchedule].
//   - err (error): non-nil when the duration is missing, invalid, or
//     non-positive.
func parseEvery(fields []string) (schedule Schedule, err error) {
	if len(fields) != 2 {
		err = fmt.Errorf("scheduler.Parse: @every requires exactly one duration argument, got %q", strings.Join(fields, " "))

		return
	}

	every, perr := time.ParseDuration(fields[1])
	if perr != nil {
		err = fmt.Errorf("scheduler.Parse: invalid @every duration %q: %w", fields[1], perr)

		return
	}

	if every <= 0 {
		err = fmt.Errorf("scheduler.Parse: @every duration must be positive, got %s", every)

		return
	}

	schedule = IntervalSchedule{Every: every}

	return
}

// parseStandard parses a standard 5-field cron expression.
//
// Parameters:
//   - spec (string): the 5-field expression.
//
// Returns:
//   - schedule (Schedule): the parsed [*specSchedule].
//   - err (error): non-nil on wrong field counts or malformed fields.
func parseStandard(spec string) (schedule Schedule, err error) {
	fields := strings.Fields(spec)

	if len(fields) != 5 {
		err = fmt.Errorf("scheduler.Parse: expected 5 fields, got %d in %q", len(fields), spec)

		return
	}

	minutes, err := parseField(fields[0], 0, 59, nil, false, "minute")
	if err != nil {
		return
	}

	hours, err := parseField(fields[1], 0, 23, nil, false, "hour")
	if err != nil {
		return
	}

	dom, err := parseField(fields[2], 1, 31, nil, true, "day-of-month")
	if err != nil {
		return
	}

	months, err := parseField(fields[3], 1, 12, monthNames, false, "month")
	if err != nil {
		return
	}

	// Day-of-week accepts both 0 and 7 for Sunday.
	dow, err := parseField(fields[4], 0, 7, dowNames, true, "day-of-week")
	if err != nil {
		return
	}

	// Fold Sunday's alias bit 7 into bit 0.
	if dow&(uint64(1)<<7) != 0 {
		dow &^= uint64(1) << 7

		dow |= uint64(1)
	}

	// parseField sets no bits outside [min, max], so the masked narrowing
	// conversions below cannot lose information.
	schedule = &specSchedule{
		minutes:       minutes,
		hours:         hours,
		dom:           uint32(dom & 0xFFFFFFFF),
		months:        uint16(months & 0xFFFF),
		dow:           uint8(dow & 0xFF),
		domRestricted: !isWildcard(fields[2]),
		dowRestricted: !isWildcard(fields[4]),
	}

	return
}

// isWildcard reports whether a field is unrestricted (`*` or `?`).
//
// Parameters:
//   - field (string): the raw field text.
//
// Returns:
//   - wildcard (bool): true when the field matches everything.
func isWildcard(field string) (wildcard bool) {
	wildcard = field == "*" || field == "?"

	return
}

// parseField parses a single cron field into a bitmask of matching values.
//
// Parameters:
//   - field (string): the raw field text.
//   - min (int): the smallest allowed value (inclusive).
//   - max (int): the largest allowed value (inclusive).
//   - names (map[string]int): optional name-to-value aliases (months,
//     days of week); may be nil.
//   - allowQuestion (bool): whether `?` is accepted as an alias for `*`.
//   - name (string): the human-readable field name used in error messages.
//
// Returns:
//   - bits (uint64): bitmask with one bit set per matching value; no bits
//     outside [min, max] are ever set.
//   - err (error): non-nil on malformed syntax or out-of-range values.
func parseField(field string, minValue, maxValue int, names map[string]int, allowQuestion bool, name string) (bits uint64, err error) {
	if field == "" {
		err = fmt.Errorf("scheduler.Parse: %s field is empty", name)

		return
	}

	for _, item := range strings.Split(field, ",") {
		if item == "" {
			err = fmt.Errorf("scheduler.Parse: %s field contains an empty list item in %q", name, field)

			return
		}

		itemBits, ierr := parseItem(item, minValue, maxValue, names, allowQuestion, name)
		if ierr != nil {
			err = ierr

			return
		}

		bits |= itemBits
	}

	return
}

// parseItem parses one comma-separated item of a cron field: `*`, `?`,
// `*/n`, `a`, `a-b`, `a/n`, `a-b/n`.
//
// Parameters:
//   - item (string): the raw item text.
//   - min (int): the smallest allowed value (inclusive).
//   - max (int): the largest allowed value (inclusive).
//   - names (map[string]int): optional name-to-value aliases; may be nil.
//   - allowQuestion (bool): whether `?` is accepted as an alias for `*`.
//   - name (string): the human-readable field name used in error messages.
//
// Returns:
//   - bits (uint64): bitmask with one bit set per matching value.
//   - err (error): non-nil on malformed syntax or out-of-range values.
func parseItem(item string, minValue, maxValue int, names map[string]int, allowQuestion bool, name string) (bits uint64, err error) {
	rangePart, step, hasStep := item, 1, false

	if strings.Contains(item, "/") {
		parts := strings.Split(item, "/")

		if len(parts) != 2 {
			err = fmt.Errorf("scheduler.Parse: %s field contains a malformed step in %q", name, item)

			return
		}

		rangePart = parts[0]

		hasStep = true

		step, err = parseNumber(parts[1], names)
		if err != nil {
			err = fmt.Errorf("scheduler.Parse: %s field contains an invalid step in %q: %w", name, item, err)

			return
		}

		if step <= 0 {
			err = fmt.Errorf("scheduler.Parse: %s field step must be positive in %q", name, item)

			return
		}
	}

	var low, high int

	switch {
	case rangePart == "*":
		low, high = minValue, maxValue
	case rangePart == "?" && allowQuestion:
		low, high = minValue, maxValue
	case strings.Contains(rangePart, "-"):
		bounds := strings.Split(rangePart, "-")

		if len(bounds) != 2 {
			err = fmt.Errorf("scheduler.Parse: %s field contains a malformed range in %q", name, item)

			return
		}

		low, err = parseNumber(bounds[0], names)
		if err != nil {
			err = fmt.Errorf("scheduler.Parse: %s field contains an invalid range bound in %q: %w", name, item, err)

			return
		}

		high, err = parseNumber(bounds[1], names)
		if err != nil {
			err = fmt.Errorf("scheduler.Parse: %s field contains an invalid range bound in %q: %w", name, item, err)

			return
		}
	default:
		low, err = parseNumber(rangePart, names)
		if err != nil {
			err = fmt.Errorf("scheduler.Parse: %s field contains an invalid value in %q: %w", name, item, err)

			return
		}

		// A bare value with a step (`a/n`) means "from a to the maximum,
		// every n".
		if hasStep {
			high = maxValue
		} else {
			high = low
		}
	}

	if low < minValue || low > maxValue {
		err = fmt.Errorf("scheduler.Parse: %s field value %d is out of range [%d, %d] in %q", name, low, minValue, maxValue, item)

		return
	}

	if high < minValue || high > maxValue {
		err = fmt.Errorf("scheduler.Parse: %s field value %d is out of range [%d, %d] in %q", name, high, minValue, maxValue, item)

		return
	}

	if low > high {
		err = fmt.Errorf("scheduler.Parse: %s field range %d-%d is inverted in %q", name, low, high, item)

		return
	}

	for value := low; value <= high; value += step {
		bits |= uint64(1) << uint(value)
	}

	return
}

// parseNumber parses a numeric cron value, resolving name aliases first.
// Signs are rejected: `+5` and `-2` are not valid cron values.
//
// Parameters:
//   - text (string): the raw value text (e.g. `5`, `JAN`, `sun`).
//   - names (map[string]int): optional name-to-value aliases; may be nil.
//
// Returns:
//   - value (int): the parsed numeric value.
//   - err (error): non-nil when the text is neither a known name nor an
//     unsigned number.
func parseNumber(text string, names map[string]int) (value int, err error) {
	if names != nil {
		if named, ok := names[strings.ToLower(text)]; ok {
			value = named

			return
		}
	}

	for _, r := range text {
		if r < '0' || r > '9' {
			err = fmt.Errorf("scheduler.Parse: %q is neither a number nor a known name", text)

			return
		}
	}

	value, err = strconv.Atoi(text)
	if err != nil {
		err = fmt.Errorf("scheduler.Parse: %q is neither a number nor a known name: %w", text, err)

		return
	}

	return
}

// specSchedule satisfies the contract.
var _ Schedule = (*specSchedule)(nil)
