# hq-lib-scheduler-go

![made with go](https://img.shields.io/badge/made%20with-Go-1E90FF.svg) [![go reference](https://pkg.go.dev/badge/github.com/hueristiq/hq-lib-scheduler-go.svg)](https://pkg.go.dev/github.com/hueristiq/hq-lib-scheduler-go) [![license](https://img.shields.io/badge/license-MIT-gray.svg?color=1E90FF)](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/LICENSE) ![maintenance](https://img.shields.io/badge/maintained%3F-yes-1E90FF.svg) [![open issues](https://img.shields.io/github/issues-raw/hueristiq/hq-lib-scheduler-go.svg?style=flat&color=1E90FF)](https://github.com/hueristiq/hq-lib-scheduler-go/issues?q=is:issue+is:open) [![closed issues](https://img.shields.io/github/issues-closed-raw/hueristiq/hq-lib-scheduler-go.svg?style=flat&color=1E90FF)](https://github.com/hueristiq/hq-lib-scheduler-go/issues?q=is:issue+is:closed) [![contribution](https://img.shields.io/badge/contributions-welcome-1E90FF.svg)](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/CONTRIBUTING.md)

`hq-lib-scheduler-go` is a [Go](https://golang.org/) package for running jobs concurrently, with graceful shutdown, on a time-based schedule.

## Resources

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
	- [Scheduling a Job](#scheduling-a-job)
	- [Descriptors & Fixed Intervals](#descriptors--fixed-intervals)
	- [Custom Schedules](#custom-schedules)
	- [Error Handling](#error-handling)
- [Cron Expression Format](#cron-expression-format)
- [Configuration](#configuration)
- [Run Semantics](#run-semantics)
- [Contributing](#contributing)
- [Licensing](#licensing)

## Features

- **Standard Cron Expressions:** Full 5-field `minute hour day-of-month month day-of-week` syntax — wildcards, steps, ranges, lists, month and day names, `?`, and both `0` and `7` for Sunday — with standard dom/dow OR-semantics.
- **Descriptors & Intervals:** `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`, and `@every <duration>` for plain intervals.
- **Concurrent Execution:** Every activation fires its job in its own goroutine, optionally bounded by a concurrency limit, while overlapping runs of the same entry are skipped.
- **Graceful Shutdown:** `Stop` cancels every per-run context and waits for in-flight runs, bounded by a caller-supplied context.
- **Failure Reporting:** Returned errors and recovered panics are delivered to a configurable error handler — a panicking job never crashes the process.
- **Introspection:** `Entries` exposes a snapshot of every registered job with its previous and next activation times.

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

Register a job on a cron expression with `Add`, start the scheduler, and stop it gracefully on shutdown:

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
		hqgoscheduler.WithErrorHandler(func(entry hqgoscheduler.Entry, err error) {
			fmt.Printf("job %q failed: %v\n", entry.Job.Name(), err)
		}),
	)

	job := hqgoscheduler.NewJob("report", func(ctx context.Context) error {
		fmt.Println("generating report...")

		return nil
	})

	// Every weekday at 09:30 UTC.
	id, err := scheduler.Add("30 9 * * MON-FRI", job)
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

Registration is also possible after `Start`, and `Remove(id)` unregisters an entry at any time.

### Descriptors & Fixed Intervals

Named descriptors cover the common cases, and `@every` (or `AddEvery`) covers plain intervals:

```go
_, _ = scheduler.Add("@hourly", job)                  // top of every hour
_, _ = scheduler.Add("@every 30s", job)               // every 30 seconds
_, _ = scheduler.AddEvery(5*time.Minute, job)         // every 5 minutes, no parsing
```

### Custom Schedules

Anything implementing the one-method `Schedule` interface can drive an entry via `AddSchedule` — including a spec parsed once and shared between entries:

```go
schedule, err := hqgoscheduler.Parse("0 0 1 * *")
if err != nil {
	panic(err)
}

_, _ = scheduler.AddSchedule(schedule, hqgoscheduler.NewJob("monthly-a", runA))
_, _ = scheduler.AddSchedule(schedule, hqgoscheduler.NewJob("monthly-b", runB))
```

### Error Handling

`WithErrorHandler` receives a snapshot of the entry and the failure — a returned error or a recovered panic — for every failed run:

```go
scheduler := hqgoscheduler.NewScheduler(
	hqgoscheduler.WithErrorHandler(func(entry hqgoscheduler.Entry, err error) {
		metrics.Incr("scheduler.job.failed", "name", entry.Job.Name())
	}),
)
```

## Cron Expression Format

A standard expression has five fields, separated by whitespace:

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12 or JAN-DEC)
│ │ │ │ ┌───────────── day of week (0-7 or SUN-SAT; 0 and 7 are Sunday)
│ │ │ │ │
* * * * *
```

| Syntax | Meaning |
| --- | --- |
| `*` | every value |
| `?` | alias for `*` in the day-of-month and day-of-week fields |
| `*/n` | every `n` values, starting from the field minimum |
| `a` | the single value `a` |
| `a-b` | every value from `a` through `b` |
| `a-b/n` | every `n` values from `a` through `b` |
| `a/n` | every `n` values from `a` through the field maximum |
| `a,b,c` | any of the listed items (items may be any of the above) |

When both the day-of-month and day-of-week fields are restricted, a day matches if **either** field matches (standard cron OR-semantics). Activations are minute-granular and are evaluated in the location configured with `WithLocation`.

## Configuration

Behavior is set through functional options passed to `NewScheduler`. Any option left unset falls back to its default.

| Option | Description | Default |
| --- | --- | --- |
| `WithLocation(loc)` | Location cron expressions are evaluated in. `nil` is ignored. | `time.Local` |
| `WithClock(fn)` | Time source of the scheduler. Exists primarily as a test seam; `nil` is ignored. | `time.Now` |
| `WithMaxConcurrency(n)` | Maximum number of job runs executing concurrently; runs beyond the bound wait for a slot. Negative values are treated as zero. | `0` (unlimited) |
| `WithErrorHandler(fn)` | Callback invoked for every failed run, with the entry snapshot and the error or recovered panic. | failures dropped |

## Run Semantics

- Each entry's next activation is computed with `Schedule.Next`, strictly after the current time; the loop sleeps until the earliest activation.
- On fire, the job runs in its own goroutine with a per-run context cancelled by `Stop`.
- Overlapping runs of the same entry are **skipped**: if the previous run is still in flight when the next activation arrives, that activation is dropped and the entry is rescheduled.
- `Stop` halts the loop, cancels per-run contexts, and waits for in-flight runs until they finish or the provided context expires.
- A stopped scheduler may be started again: `Start` after `Stop` launches a fresh loop and reschedules every entry from the current time.

## Contributing

Contributions are welcome and encouraged! Feel free to submit [Pull Requests](https://github.com/hueristiq/hq-lib-scheduler-go/pulls) or report [Issues](https://github.com/hueristiq/hq-lib-scheduler-go/issues). For more details, check out the [contribution guidelines](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/CONTRIBUTING.md).

A big thank you to all the [contributors](https://github.com/hueristiq/hq-lib-scheduler-go/graphs/contributors) for your ongoing support!

![contributors](https://contrib.rocks/image?repo=hueristiq/hq-lib-scheduler-go&max=500)

## Licensing

This package is licensed under the [MIT license](https://opensource.org/license/mit). You are free to use, modify, and distribute it, as long as you follow the terms of the license. You can find the full license text in the repository - [Full MIT license text](https://github.com/hueristiq/hq-lib-scheduler-go/blob/main/LICENSE).
