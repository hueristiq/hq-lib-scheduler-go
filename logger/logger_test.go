package logger_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hueristiq/hq-lib-scheduler-go/logger"
)

func TestNop(t *testing.T) {
	t.Parallel()

	var nop logger.Logger = logger.Nop{}

	// Every level must accept a call — bare and with attributes — without
	// panicking.
	require.NotPanics(t, func() {
		nop.Debug("debug")
		nop.Info("info", "key", "value")
		nop.Warn("warn", "entry_id", 42, "job", "report")
		nop.Error("error", "error", errors.New("boom"), "duration", time.Second)
	})
}
