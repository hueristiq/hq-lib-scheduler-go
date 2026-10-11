# hq-lib-scheduler-go

![made with go](https://img.shields.io/badge/made%20with-Go-1E90FF.svg) [![go reference](https://pkg.go.dev/badge/github.com/hueristiq/hq-lib-scheduler-go.svg)](https://pkg.go.dev/github.com/hueristiq/hq-lib-scheduler-go) [![license](https://img.shields.io/badge/license-MIT-gray.svg?color=1E90FF)](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/LICENSE) ![maintenance](https://img.shields.io/badge/maintained%3F-yes-1E90FF.svg) [![open issues](https://img.shields.io/github/issues-raw/hueristiq/hq-lib-scheduler-go.svg?style=flat&color=1E90FF)](https://github.com/hueristiq/hq-lib-scheduler-go/issues?q=is:issue+is:open) [![closed issues](https://img.shields.io/github/issues-closed-raw/hueristiq/hq-lib-scheduler-go.svg?style=flat&color=1E90FF)](https://github.com/hueristiq/hq-lib-scheduler-go/issues?q=is:issue+is:closed) [![contribution](https://img.shields.io/badge/contributions-welcome-1E90FF.svg)](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/CONTRIBUTING.md)

`hq-lib-scheduler-go` is a [Go](https://golang.org/) package for running jobs on a time-based schedule.

## Resources

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
	- [Scheduling a Job](#scheduling-a-job)
	- [Custom Schedules](#custom-schedules)
	- [Descriptors & Fixed Intervals](#descriptors--fixed-intervals)
	- [Error Handling](#error-handling)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [Licensing](#licensing)

## Features

- **Standard Cron Expressions Support:** Full 5-field syntax with wildcards, steps, ranges, lists, names, and dom/dow OR-semantics.
- **Descriptors & Intervals Support:** `@daily`, `@hourly`, and friends, plus `@every <duration>` for plain intervals.
- **Custom Schedules:** `RegisterSchedule` accepts any `Schedule` implementation — from `Parse`, the built-in `IntervalSchedule`, or your own type.
- **Concurrent Execution:** Each activation runs in its own goroutine; overlapping runs are skipped.
- **Introspection:** `Entries` snapshots registered jobs with previous and next activation times.
- **Graceful Shutdown:** `Stop` cancels run contexts and waits for in-flight jobs.

## Installation

To install `hq-lib-scheduler-go`, run the following command in your Go project:

```bash
go get -v -u github.com/hueristiq/hq-lib-scheduler-go
```

## Usage

The examples below import the package under the `hqgoscheduler` alias.

```go
import hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
```

### Scheduling a Job

Schedule a job on a cron expression with `Register`, start the scheduler, and stop it gracefully on shutdown:

```go
package main

import (
	"context"
	"fmt"
	"time"

	hqgoscheduler "github.com/hueristiq/hq-lib-scheduler-go"
)

func main() {
	scheduler := hqgoscheduler.NewScheduler(
		hqgoscheduler.WithLocation(time.UTC),
		hqgoscheduler.WithOnError(func(event hqgoscheduler.ErrorEvent) {
			fmt.Printf("job %q failed: %v\n", event.Entry.Job.Name(), event.Err)
		}),
	)

	// Every weekday at 09:30 UTC.
	schedule := "30 9 * * MON-FRI"

	job := hqgoscheduler.NewJob("report", func(ctx context.Context) error {
		fmt.Println("generating report...")

		return nil
	})

	id, err := scheduler.Register(schedule, job)
	if err != nil {
		panic(err)
	}

	fmt.Println("registered entry", id)

	scheduler.Start(context.Background())

	// ... run the application ...

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	if err := scheduler.Stop(ctx); err != nil {
		fmt.Printf("shutdown incomplete: %v\n", err)
	}
}
```

Registration is also possible after `Start`, and `Unregister(id)` unregisters an entry at any time.

### Custom Schedules

`Register` covers cron expressions, descriptors, and `@every` intervals. For a pre-built `Schedule` — from `Parse`, the built-in `IntervalSchedule`, or a custom implementation — use `RegisterSchedule`:

```go
schedule, err := hqgoscheduler.Parse("30 9 * * MON-FRI")
if err != nil {
	panic(err)
}

id, err := scheduler.RegisterSchedule(schedule, job)
if err != nil {
	panic(err)
}
```

### Descriptors & Fixed Intervals

Named descriptors cover the common cases, and `@every` covers plain intervals:

```go
_, _ = scheduler.Register("@hourly", job)                  // top of every hour
_, _ = scheduler.Register("@every 30s", job)               // every 30 seconds
```

### Error Handling

`WithOnError` receives an `ErrorEvent` — a snapshot of the entry and the failure, a returned error or a recovered panic — for every failed run:

```go
scheduler := hqgoscheduler.NewScheduler(
	hqgoscheduler.WithOnError(func(event hqgoscheduler.ErrorEvent) {
		metrics.Incr("scheduler.job.failed", "name", event.Entry.Job.Name())
	}),
)
```

## Configuration

Behavior is set through functional options passed to `NewScheduler`. Any option left unset falls back to its default.

| Option | Description | Default |
| --- | --- | --- |
| `WithLocation(loc)` | Location cron expressions are evaluated in. `nil` is ignored. | `time.Local` |
| `WithClock(fn)` | Time source of the scheduler. Exists primarily as a test seam; `nil` is ignored. | `time.Now` |
| `WithMaxConcurrency(n)` | Maximum number of job runs executing concurrently; runs beyond the bound wait for a slot. Negative values are treated as zero. | `0` (unlimited) |
| `WithOnError(fn)` | Callback invoked for every failed run, receiving a ErrorEvent with the entry snapshot and the error or recovered panic. | failures dropped |

## Contributing

Contributions are welcome and encouraged! Feel free to submit [Pull Requests](https://github.com/hueristiq/hq-lib-scheduler-go/pulls) or report [Issues](https://github.com/hueristiq/hq-lib-scheduler-go/issues). For more details, check out the [contribution guidelines](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/CONTRIBUTING.md).

A big thank you to all the [contributors](https://github.com/hueristiq/hq-lib-scheduler-go/graphs/contributors) for your ongoing support!

![contributors](https://contrib.rocks/image?repo=hueristiq/hq-lib-scheduler-go&max=500)

## Licensing

This package is licensed under the [MIT license](https://opensource.org/license/mit). You are free to use, modify, and distribute it, as long as you follow the terms of the license. You can find the full license text in the repository - [Full MIT license text](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/LICENSE).
