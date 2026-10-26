package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartOfNextMonthHandlesMidnightGap(t *testing.T) {
	t.Parallel()

	loc, err := time.LoadLocation("America/Asuncion")
	require.NoError(t, err)

	// Paraguay's 2017 spring-forward transition was at local midnight on
	// 2017-10-01, so 00:00 does not exist and time.Date normalizes it back to
	// September 30. The helper must still land on October 1.
	after := time.Date(2017, time.September, 15, 12, 0, 0, 0, loc)
	next := startOfNextMonth(after)

	require.Equal(t, 2017, next.Year())
	require.Equal(t, time.October, next.Month())
	require.Equal(t, 1, next.Day())
	require.Equal(t, 1, next.Hour())
	require.True(t, next.After(after))
}
