# Real Time Job Queue

A small, persistent job queue with retries, dead letter handling, and
idempotency, built in small daily commits as big tech interview prep.
The point is not just a working queue, it is the failure modes, retry, and
durability decisions recorded along the way, so you can talk about them in an
interview.

**Status:** In progress, one feature per day, pushed daily.
Day 1 is done. The project is finished when every item below is checked off.

## What it does so far
- `POST /jobs`, enqueue a job with a handler name, a payload, and an optional
  idempotency key
- `GET /jobs/{id}`, job status, attempts, and result or error
- `GET /jobs`, list jobs, newest first
- `GET /stats`, counts by status
- A worker that claims a job, runs its handler, retries on failure up to the
  max attempts, then moves it to the dead letter status
- Same idempotency key returns the same job, never a duplicate

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
- [ ] Day 2, Delayed jobs, run at a future time
- [ ] Day 3, Priority queue
- [ ] Day 4, Multiple workers, concurrency
- [ ] Day 5, Job progress and cancellation
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
