package scheduler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

func TestJob_Adapter(t *testing.T) {
	t.Parallel()

	job := hqgoscheduler.NewJob("greeter", hqgoscheduler.JobFunc(func(ctx context.Context) (err error) {
		return
	}))

	require.Equal(t, "greeter", job.Name())
	require.NoError(t, job.Run(t.Context()))
}
