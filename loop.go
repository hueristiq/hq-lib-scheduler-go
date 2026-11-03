package scheduler

import (
	"container/heap"
	"context"
	"time"
)

// loop is the scheduling loop: it fires every due entry, recomputes the
// affected schedules, then sleeps until the earliest scheduled activation, a
// wake-up signal, or shutdown.
//
// Scheduled entries live in a min-heap keyed on their next activation, so an
// iteration touches only the entries that are actually due instead of
// scanning every registered entry. [Schedule.Next] is user-supplied code and
// is NEVER invoked while holding s.mu — a schedule calling back into the
// scheduler must not deadlock it: due entries are popped and unevaluated
// entries collected under the lock, their next activations are computed
// after the lock is released, and the results are applied under a second
// critical section that skips entries removed in the meantime and applies
// nothing when the loop was superseded (the stop that superseded it already
// re-queued every entry). An entry whose schedule reports no future
// activation stays out of the heap until the next [Scheduler.Stop]–
// [Scheduler.Start] cycle re-evaluates every entry.
//
// The generation identifies the [Scheduler.Start] call the loop belongs to:
// when a restart supersedes it, the loop exits instead of double-firing
// jobs. loopDone is closed on exit, runCtx parents every per-run context.
// The sleep timer is created lazily on the first sleep and reused via Reset
// instead of reallocated per iteration.
func (s *Scheduler) loop(runCtx context.Context, generation int, loopDone chan struct{}) {
	defer close(loopDone)
	defer func() {
		s.mu.Lock()

		// Only the generation that owns this loop may clear the flag; a
		// concurrent Start may already have installed a newer loop.
		if s.generation == generation {
			s.started = false
		}

		s.mu.Unlock()
	}()

	var timer *time.Timer

	for {
		// Sample the clock before taking the mutex: s.now calls the
		// user-supplied clock, which must never run under s.mu.
		now := s.now()

		s.mu.Lock()

		if !s.started || s.generation != generation || runCtx.Err() != nil {
			s.mu.Unlock()

			return
		}

		// Pop every due entry off the heap; a popped entry stays out of the
		// heap until its next activation is recomputed and applied below.
		var due []*entry

		for len(s.queue) > 0 && !s.queue[0].next.After(now) {
			e := heap.Pop(&s.queue).(*entry)

			due = append(due, e)
		}

		// Fire each due entry in its own goroutine, dropping the activation
		// when the previous run is still in flight. Fired or dropped, the
		// entry is rescheduled from now, outside the lock.
		reschedule := make([]*entry, 0, len(due)+len(s.pending))

		for _, e := range due {
			if !e.running {
				e.running = true
				e.previous = e.next

				s.wg.Add(1)

				go s.runJob(runCtx, e.id, e.job)
			}

			e.next = time.Time{}

			reschedule = append(reschedule, e)
		}

		// Collect the entries awaiting their first schedule evaluation —
		// newly added ones and every entry reset by [Scheduler.Stop].
		for _, e := range s.pending {
			e.queued = false

			reschedule = append(reschedule, e)
		}

		s.pending = nil

		s.mu.Unlock()

		// Compute the next activations WITHOUT holding the lock:
		// [Schedule.Next] is user code and may call back into the scheduler.
		updates := make([]nextActivation, 0, len(reschedule))

		for _, e := range reschedule {
			updates = append(updates, nextActivation{entry: e, next: e.schedule.Next(now)})
		}

		s.mu.Lock()

		// Apply the recomputed activations, skipping entries removed while
		// the lock was released.
		current := s.started && s.generation == generation && runCtx.Err() == nil

		if current {
			for _, u := range updates {
				e, ok := s.entries[u.entry.id]
				if !ok || e != u.entry {
					continue
				}

				e.next = u.next

				if !e.next.IsZero() {
					heap.Push(&s.queue, e)
				}
			}
		}

		var earliest time.Time

		if len(s.queue) > 0 {
			earliest = s.queue[0].next
		}

		s.mu.Unlock()

		var timerC <-chan time.Time

		if !earliest.IsZero() {
			delay := earliest.Sub(s.now())

			if delay < 0 {
				delay = 0
			}

			// Cap the idle sleep so time sources substituted through
			// [WithClock] — which may jump forward arbitrarily — are
			// re-checked at least once per quantum.
			if delay > maxSleepQuantum {
				delay = maxSleepQuantum
			}

			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				timer.Reset(delay)
			}

			timerC = timer.C
		}

		select {
		case <-runCtx.Done():
			if timer != nil {
				timer.Stop()
			}

			return
		case <-s.wake:
			if timer != nil {
				timer.Stop()
			}
		case <-timerC:
		}
	}
}

// runJob executes one run of the entry identified by id in its own
// goroutine, honoring the concurrency semaphore, recovering panics, and
// reporting failures to the configured [OnError] callback.
//
// Parameters:
//   - parent (context.Context): the scheduler run context the per-run
//     context derives from.
//   - id (int): the entry identifier (used to clear the in-flight flag).
//   - job (Job): the unit of work to execute.
func (s *Scheduler) runJob(parent context.Context, id int, job Job) {
	defer s.wg.Done()

	name, nameErr := safeJobName(job)
	if nameErr != nil {
		snapshot := s.finishRun(id, job)

		if s.onError != nil {
			deliverError(s.onError, &ErrorEvent{Entry: snapshot, Err: nameErr})
		}

		return
	}

	ctx, cancel := context.WithCancel(parent)

	defer cancel()

	if s.sem != nil {
		select {
		case s.sem <- struct{}{}:
			// The slot became available in the same instant shutdown
			// started. [Scheduler.Stop] cancels the run context after
			// clearing s.started, so re-check both before running: a run
			// admitted only after shutdown must not execute.
			if ctx.Err() != nil || !s.isStarted() {
				<-s.sem

				s.finishRun(id, job)

				return
			}

			defer func() { <-s.sem }()
		case <-ctx.Done():
			// Shutdown while waiting for a concurrency slot: release the
			// in-flight flag without running.
			s.finishRun(id, job)

			return
		}
	}

	if ctx.Err() != nil || !s.isStarted() {
		s.finishRun(id, job)

		return
	}

	err := runJobSafely(ctx, job, name)

	snapshot := s.finishRun(id, job)

	if err != nil && s.onError != nil {
		deliverError(s.onError, &ErrorEvent{Entry: snapshot, Err: err})
	}
}

// finishRun clears the in-flight flag of the entry identified by id and
// returns a snapshot of it for failure reporting. When the entry was removed
// while the run was in flight, the snapshot carries only the identifier and
// the job.
//
// Parameters:
//   - id (int): the entry identifier.
//   - job (Job): the job that ran, used for the fallback snapshot.
//
// Returns:
//   - snapshot (Entry): the entry snapshot.
func (s *Scheduler) finishRun(id int, job Job) (snapshot Entry) {
	snapshot = Entry{ID: id, Job: job}

	s.mu.Lock()

	defer s.mu.Unlock()

	if e, ok := s.entries[id]; ok {
		e.running = false

		snapshot = e.snapshot()
	}

	return
}

// isStarted reports whether the scheduling loop is currently running.
//
// Returns:
//   - started (bool): true while the scheduler is started.
func (s *Scheduler) isStarted() (started bool) {
	s.mu.Lock()

	defer s.mu.Unlock()

	started = s.started

	return
}

// ErrorEvent carries the data of one failed job run, delivered to the
// [OnError] callback configured with [WithOnError].
//
// Fields:
//   - Entry (Entry): snapshot of the failed entry. When the entry was removed
//     while the run was in flight, the snapshot carries only the identifier and
//     the job.
//   - Err (error): failure — the error the job returned, or its recovered
//     panic; never nil.
