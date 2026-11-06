# Real Time Job Queue

A small, persistent job queue with retries, dead letter handling, and
idempotency, built in small daily commits as big tech interview prep.
The point is not just a working queue, it is the failure modes, retry, and
durability decisions recorded along the way, so you can talk about them in an
interview.

**Status:** In progress, one feature per day, pushed daily.
Day 5 is done, 2026-11-06. The project is finished when every item below is checked off.

## What it does so far
- `POST /jobs`, enqueue a job with a handler name, a payload, and an optional
  idempotency key
- `GET /jobs/{id}`, job status, attempts, and result or error
- `GET /jobs`, list jobs, newest first
- `GET /stats`, counts by status
- A worker that claims a job, runs its handler, retries on failure up to the
  max attempts, then moves it to the dead letter status
- Same idempotency key returns the same job, never a duplicate
- `POST /jobs` accepts run_at or delay_seconds, and the worker skips jobs
  whose run time has not arrived yet, so delayed jobs never run early
- `POST /jobs` accepts priority, default 0. Among due jobs, higher priority
  runs first, then earlier run_at, then lower id, so priority never lets a
  future job run early
- Several workers can run at once, each in its own thread, and the atomic
  claim means no job is ever run twice. Start 4 workers with
  `python -m queue.workers 4`, and each job records which worker claimed it
  in claimed_by
- Handlers can report progress from 0 to 100 with a message, stored on the
  job row, so `GET /jobs/{id}` shows live progress while a job runs
- `POST /jobs/{id}/cancel` cancels a pending job at once, so it never runs,
  and flags a running job for cooperative cancellation, so it stops at the
  next step and ends as cancelled instead of completed

## Stack
Python, FastAPI, SQLite for job persistence.

## Run it
```bash
pip install -r requirements.txt
uvicorn app.main:app
```

## Run tests
```bash
pytest
```

## Daily build plan (one feature plus tests per day)
- [x] Day 1, Core queue, SQLite, worker, retry, dead letter, idempotency
- [x] Day 2, Delayed jobs, run at a future time, 2026-11-03
- [x] Day 3, Priority queue, 2026-11-04
- [x] Day 4, Multiple workers, concurrency, 2026-11-05
- [x] Day 5, Job progress and cancellation, 2026-11-06
- [ ] Day 6, Dead letter inspection and requeue endpoint
- [ ] Day 7, Persistence recovery on restart
- [ ] Day 8, Rate limiting per handler
- [ ] Day 9, Job timeouts
- [ ] Day 10, Dashboard stats endpoint
- [ ] Day 11, Batch enqueue
- [ ] Day 12, Backoff strategies, exponential backoff
- [ ] Day 13, Poison job handling, failure stories
- [ ] Day 14, Load test, throughput numbers
- [ ] Day 15, System design writeup, docs/system-design.md
- [ ] Day 16, Scheduled recurring jobs
- [ ] Day 17, Final polish, interview talking points, project complete

## Design decisions (log them here as you go)
- Jobs live in SQLite, not just in memory, so a restart does not lose the
  queue. The worker claims a job by marking it running before it runs the
  handler, so a crash leaves it visible instead of silently lost.
- Retries are counted on the job, not in the worker, so the retry policy is
  part of the job record and easy to inspect.
- Idempotency is enforced on the key at enqueue time. A second enqueue with
  the same key returns the original job, which is the safe behaviour for
  clients that retry a request.
- Delayed jobs are stored with a run_at time in the same SQLite row, not in
  a separate timer. The worker only claims due jobs, ordered by run_at, so a
  delayed job survives a restart and still runs at the right time, and a
  future job cannot jump ahead of a due job.
- Priority is a plain integer column on the same job row, default 0, with
  higher meaning sooner. Ordering is done in the claim query itself, by
  priority, then run_at, then id, so the worker needs no in memory sort and
  the ordering rule lives in one place.
- Claiming is one atomic UPDATE with a subquery and RETURNING, not a select
  followed by an update, so two workers racing for the same job cannot both
  win it. SQLite serialises the writers, WAL mode and a 30 second busy
  timeout let workers wait their turn instead of failing, and claimed_by on
  the job row records which worker won, which makes duplicate runs visible
  in tests and in production.
- Progress is a plain percent and message on the same job row, written by a
  reporter the worker hands to handlers that ask for one, so polling
  `GET /jobs/{id}` is the progress API and no separate channel is needed.
  Cancellation is cooperative, not a thread kill, because killing a thread
  mid handler can corrupt shared state. A pending job is cancelled at once,
  a running job gets a cancel_requested flag its handler checks each step,
  and the worker marks it cancelled instead of completed when the handler
  returns, so a cancelled job is never retried or dead lettered.
