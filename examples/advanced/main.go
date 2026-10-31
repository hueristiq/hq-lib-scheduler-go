package main

import (
	"context"
	"fmt"
	"time"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

func main() {
	scheduler := hqgoscheduler.New()

	entryID, err := scheduler.RegisterSchedule(
		hqgoscheduler.IntervalSchedule{Every: time.Hour},
		hqgoscheduler.NewJob("tick", func(ctx context.Context) error {
			return nil
		}),
	)
	if err != nil {
		fmt.Println("register:", err)

		return
	}

	ctx := context.Background()
	scheduler.Start(ctx)

	if err := scheduler.Stop(ctx); err != nil {
		fmt.Println("stop:", err)
	}

	fmt.Println(entryID)
}
