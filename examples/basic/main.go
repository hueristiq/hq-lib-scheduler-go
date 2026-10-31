package main

import (
	"context"
	"fmt"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

func main() {
	scheduler := hqgoscheduler.New()

	entryID, err := scheduler.Register("@every 1h", hqgoscheduler.NewJob("noop", func(ctx context.Context) error {
		return nil
	}))
	if err != nil {
		fmt.Println("register:", err)

		return
	}

	scheduler.Start(context.Background())
	fmt.Println("registered entry:", entryID)

	if err := scheduler.Stop(context.Background()); err != nil {
		fmt.Println("stop:", err)
	}
}
