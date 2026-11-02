# hq-lib-scheduler-go

![made with go](https://img.shields.io/badge/made%20with-Go-1E90FF.svg) [![go reference](https://pkg.go.dev/badge/github.com/hueristiq/hq-lib-scheduler-go.svg)](https://pkg.go.dev/github.com/hueristiq/hq-lib-scheduler-go) [![license](https://img.shields.io/badge/license-MIT-gray.svg?color=1E90FF)](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/LICENSE) ![maintenance](https://img.shields.io/badge/maintained%3F-yes-1E90FF.svg) [![open issues](https://img.shields.io/github/issues-raw/hueristiq/hq-lib-scheduler-go.svg?style=flat&color=1E90FF)](https://github.com/hueristiq/hq-lib-scheduler-go/issues?q=is:issue+is:open) [![closed issues](https://img.shields.io/github/issues-closed-raw/hueristiq/hq-lib-scheduler-go.svg?style=flat&color=1E90FF)](https://github.com/hueristiq/hq-lib-scheduler-go/issues?q=is:issue+is:closed)

`hq-lib-scheduler-go` is a [Go](https://golang.org/) package for running jobs on a time-based schedule.

## Resources

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
	- [Scheduling a job](#scheduling-a-job)
	- [Custom schedules](#custom-schedules)
	- [Introspection](#introspection)
	- [Descriptors and fixed intervals](#descriptors-and-fixed-intervals)
	- [Error handling](#error-handling)
- [Contributing](#contributing)
- [Licensing](#licensing)

## Features

- **Cron expressions:** full 5-field syntax with wildcards, steps, ranges, lists, names, and dom/dow OR semantics.
- **Descriptors and intervals:** `@daily`, `@hourly`, and friends, plus `@every <duration>`.
- **Custom schedules:** `RegisterSchedule` accepts `Parse`, the built-in `IntervalSchedule`, or any `Schedule` implementation.
- **Concurrent execution:** each activation runs in its own goroutine, and overlapping runs are skipped.
- **Introspection:** `Entries` snapshots jobs with previous and next activation times.
- **Graceful shutdown:** `Stop` cancels run contexts and waits for in-flight jobs up to the deadline.

## Installation

To install `hq-lib-scheduler-go`, run the following command in your Go project:

```bash
go get github.com/hueristiq/hq-lib-scheduler-go@latest
```

Requires Go 1.22 or later.

## Usage

The examples import the package under the `hqgoscheduler` alias.

### Scheduling a job

Schedule a job, start the scheduler, and stop it gracefully on shutdown.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

func main() {
	scheduler := hqgoscheduler.New(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithOnError(func(event hqgoscheduler.ErrorEvent) {
			fmt.Printf("job %q failed: %v\n", event.Entry.Job.Name(), event.Err)
		}),
	)

	job := hqgoscheduler.NewJob("report", func(ctx context.Context) error {
		fmt.Println("generating report")

		return nil
	})

	id, err := scheduler.Register("@every 1s", job)
	if err != nil {
		log.Fatal(err)
	}

	if _, err = scheduler.RegisterFunc("@hourly", "cleanup", func(ctx context.Context) error {
		fmt.Println("cleaning up")

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	scheduler.Start(context.Background())
	fmt.Println("registered entry", id)

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := scheduler.Stop(ctx); err != nil {
		log.Fatal(err)
	}
}
```

Registration is also possible after `Start`, and `Unregister(id)` removes an entry at any time. `Start` and `Stop` must not be called concurrently; serialize lifecycle calls. Cancelling the lifecycle context stops the loop without waiting for in-flight runs. Use a `Stop` context that lets the in-flight runs finish before restarting. A `Stop` that times out leaves the scheduler stopped with entries not reset, so recreate the scheduler instead of restarting it.

### Custom schedules

`Register` covers cron expressions, descriptors, and `@every` intervals. For a pre-built `Schedule`, from `Parse`, the built-in `IntervalSchedule`, or a custom implementation, use `RegisterSchedule`.

```go
schedule, err := hqgoscheduler.Parse("30 9 * * MON-FRI")
if err != nil {
	log.Fatal(err)
}

id, err := scheduler.RegisterSchedule(schedule, job)
if err != nil {
	log.Fatal(err)
}
```

### Introspection

`Entries` returns a snapshot of the registered jobs, ordered by identifier.

```go
for _, entry := range scheduler.Entries() {
	fmt.Printf("%d %s next=%s\n", entry.ID, entry.Job.Name(), entry.Next.Format(time.RFC3339))
}
```

`Next` is the zero time for an entry that has not been evaluated yet or has no future activation.

### Descriptors and fixed intervals

Named descriptors cover the common cases, and `@every` covers plain intervals.

```go
_, _ = scheduler.Register("@hourly", job)
_, _ = scheduler.Register("@every 30s", job)
```

### Error handling

`WithOnError` receives an `ErrorEvent` for every failed run. The event holds a snapshot of the entry and the failure, whether the job returned an error or panicked.

```go
scheduler := hqgoscheduler.New(
	hqgoscheduler.WithOnError(func(event hqgoscheduler.ErrorEvent) {
		metrics.Incr("scheduler.job.failed", "name", event.Entry.Job.Name())
	}),
)
```

## Contributing

Contributions are welcome and encouraged! Feel free to submit [Pull Requests](https://github.com/hueristiq/hq-lib-scheduler-go/pulls) or report [Issues](https://github.com/hueristiq/hq-lib-scheduler-go/issues).

A big thank you to all the [contributors](https://github.com/hueristiq/hq-lib-scheduler-go/graphs/contributors) for your ongoing support!

![contributors](https://contrib.rocks/image?repo=hueristiq/hq-lib-scheduler-go&max=500)

## Licensing

This package is licensed under the [MIT license](https://opensource.org/license/mit). You are free to use, modify, and distribute it, as long as you follow the terms of the license. You can find the full license text in the repository - [Full MIT license text](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/LICENSE).
