# Real Time Job Queue, System Design

A small, persistent job queue in Python, FastAPI, and SQLite, built one
feature per day as interview prep. This document records how the system
works, why each piece is shaped the way it is, what it can and cannot do,
and what would change if it had to serve real production traffic.

## Goals

- A job is never lost, a crash, a restart, or a hung handler must leave
  the job in a visible state, never silently dropped.
- A job is never run twice by two workers at the same time.
- A client that retries an enqueue must not create a duplicate job.
- Every failure is inspectable, the queue keeps the error, the attempt
  history, and the dead jobs, so debugging never needs the database shell.
- The whole thing stays small enough to read in one sitting, one SQLite
  file, one jobs module, one API module, one worker module.

## Non-goals

- This is not a distributed queue. There is one SQLite file, one machine,
  and workers are threads in one process, or separate processes on the
  same machine sharing the file.
- There is no authentication, no multi-tenancy, and no priority
  preemption. A running job is never interrupted for a higher priority one.
- Handlers are Python functions registered in the process, not remote
  services, so a handler cannot be deployed on its own.

## Architecture

```
client ── HTTP ──► FastAPI app (app/main.py)
                        │
                        ▼
               JobQueue (queue/jobs.py)
                        │
                        ▼
               SQLite, one file (jobs.db)
                        ▲
                        │
               workers (queue/workers.py)
```

The API only enqueues, inspects, cancels, and requeues. It never runs a
handler. Workers poll the queue, claim one job at a time, run its handler,
and record the outcome. The database is the only shared state, so the API
and the workers can run in separate processes and still agree, as long as
they share the file.

## Data model

Four tables, all in one SQLite file.

### jobs

One row per job. The columns are the job record, and the retry policy is
part of that record, not hidden in the worker.

- `id`, autoincrement primary key, also the tiebreaker for claim order
- `handler`, name of the registered handler, `payload`, JSON text
- `status`, one of pending, running, completed, dead, cancelled
- `attempts`, `max_attempts`, default 3
- `idempotency_key`, unique, a second enqueue with the same key returns
  the original job
- `result`, `error`, JSON text for the result, plain text for the error
- `created_at`, `run_at`, unix times, `run_at` is when the job becomes due
- `priority`, integer, default 0, higher runs first
- `claimed_by`, which worker holds the job, null when unclaimed
- `progress`, 0 to 100, `progress_message`, live progress from the handler
- `cancel_requested`, flag a running job checks each step
- `timeout_seconds`, per job limit, null means no limit
- `finished_at`, set when a job reaches completed, dead, or cancelled
- `backoff_strategy`, `backoff_base_seconds`, `backoff_cap_seconds`,
  defaults none, 1.0 seconds, 60.0 seconds
- `poison`, flag set when a job fails the same way 3 times in a row

New columns are added with `ALTER TABLE` checks on startup, so a database
created by an earlier day keeps working after a later day adds a column.

### rate_limits

One row per handler, `handler` primary key, `max_per_window`, and
`window_seconds`. A handler with no row here is unlimited.

### rate_limit_runs

One row per handler start, `handler` and `started_at`. The worker counts
rows inside the window to decide whether budget remains, and prunes rows
older than the window when it checks, so the table stays small.

### job_failures

One row per failed attempt, `job_id`, `attempt`, `error`, `failed_at`.
This is the failure story, including worker crashes found on restart, so
a job that fails the same way every time is easy to tell apart from one
that fails a new way each time.

## API surface

- `POST /jobs`, enqueue one job, returns the job, or the original job
  when the idempotency key already exists
- `POST /jobs/batch`, enqueue up to 100 jobs in one transaction, all or
  nothing, each spec keeps its own handler, payload, priority, delay,
  and timeout
- `GET /jobs`, list jobs, newest first
- `GET /jobs/{id}`, one job with status, attempts, result or error
- `GET /jobs/{id}/failures`, the failure story, with distinct errors
  and the poison flag
- `POST /jobs/{id}/cancel`, cancel a pending job at once, flag a running
  job for cooperative cancellation
- `POST /jobs/{id}/requeue`, put a dead job back with a fresh attempt
  budget, a clean failure story, and its poison flag cleared
- `GET /dead`, dead letter jobs, newest first, with the error that
  killed each one
- `GET /poison`, jobs flagged as poison
- `GET /stats`, counts by status
- `GET /dashboard`, one summary for a dashboard, counts by status and
  by handler, backlog split into due now and scheduled for later, age of
  the oldest pending job, throughput for the last minute and the last
  hour, newest dead jobs, live rate limit budgets, poison count, and
  the last recovery summary
- `POST /rate-limits`, `GET /rate-limits`, `GET /rate-limits/{handler}`,
  configure and inspect per handler limits
- `GET /recovery`, what the last startup recovered
- `POST /process-next`, run one job synchronously, used by tests and by
  a single process deployment

## Job lifecycle

```
pending ── claim ──► running ── handler ok ──► completed
   ▲                    │  handler fails, attempts left
   │                    ▼
   │                 pending, run_at moved out by the backoff
   │                    │  handler fails, no attempts left
   │                    ▼
   │                  dead ── requeue ──► pending
   │
   └── cancel (pending) ──► cancelled
   running ── cancel requested, handler stops ──► cancelled
```

Pending is the only claimable state. A job leaves running only through
the worker that claimed it, by completing, failing, timing out, or being
cancelled, or through recovery on startup when its worker died. There is
no path from running back to pending except recovery, so a job can never
be claimed twice while it is running.

## Claiming and concurrency

Claiming is one atomic `UPDATE` with a subquery and `RETURNING`, not a
select followed by an update. The subquery picks one due job, ordered by
priority descending, then `run_at` ascending, then `id` ascending, and
the update marks it running and records `claimed_by` in the same
statement. Two workers racing for the same job cannot both win it,
because SQLite serialises the writers and only one update returns a row.

SQLite runs in WAL mode with a 30 second busy timeout, so workers wait
their turn instead of failing when another worker holds the write lock.
Each worker polls every 0.05 seconds when the queue is empty. Workers are
threads in one process, started with `python -m queue.workers 4`, and
`claimed_by` records which worker won each job, which makes duplicate
runs visible in tests and in production.

The cost of this simplicity is the claim itself. The claim is one write,
and SQLite allows one writer at a time, so the claim is the bottleneck
when handlers are trivial. Measured on the dev machine, 2026-11-20,
500 trivial sum jobs ran at about 361 jobs per second with 4 workers and
about 399 with 1 worker, so extra workers do not help when the handler
is trivial. With a handler that sleeps 50 ms per job, 4 workers ran at
about 67 jobs per second against about 19 with 1 worker, about 3.6x
faster, so the workers pay off when handlers do real work.

## Delayed jobs and priority

A delayed job is a normal job with `run_at` in the future, stored on the
same row, not in a separate timer. The claim query only picks jobs whose
`run_at` has arrived, so a delayed job survives a restart and still runs
at the right time. Priority never lets a future job run early, because
the due check comes first in effect, the ordering is priority, then
`run_at`, then id, applied only to due jobs.

## Retries and backoff

A failed job goes back to pending with its attempt count raised, until
it runs out of attempts and is dead lettered. The retry policy lives on
the job row, so it survives a restart like every other job setting.

Backoff is per job, with four strategies, chosen at enqueue time.

- none, the default, retry at once, so jobs enqueued without backoff
  keep the old behaviour
- exponential, wait base, then 2x base, then 4x base
- linear, add one base each attempt
- constant, wait the same base every time

The delay is a plain function of the attempt number, base 1.0 seconds
by default, capped at 60.0 seconds, so the wait for any attempt is easy
to predict and test. A failed job goes back to pending with `run_at`
moved to now plus the delay, so the worker needs no timer and no new
state, the claim query already skips jobs whose run time has not
arrived, and the dashboard already counts a waiting job as scheduled
for later. Exponential is the usual default to reach for, because it
gives a struggling downstream service room to recover while still
retrying soon after a short outage, and the cap keeps the worst case
wait bounded. A requeued job keeps its backoff settings, so the same
policy applies when it runs again.

## Timeouts

A timeout is a per job seconds limit stored on the job row. The worker
runs each handler in a daemon thread and waits at most the timeout for
it, so a handler that hangs is failed with a timed out error and retried
or dead lettered by the same rules as any other failure. Python cannot
kill a thread, so a timed out handler keeps running in the background
until it finishes, but the worker has already moved on, so a stuck
handler can never block the queue or another worker. The timeout counts
per attempt, so a retried job gets the full budget again, and a requeued
job keeps its timeout.

## Poison jobs

A poison job is one that fails with the same error 3 times in a row,
which means the failure is deterministic, a bad payload, a missing
handler, or a handler that always times out, so retrying it can never
help. It is dead lettered early with a poison flag and an error that
names the repeated error, instead of burning its remaining attempts and
backoff waits. The threshold is 3, not 2, so a job that fails twice
while a fix is being deployed is not cut off early, and jobs with only
3 attempts are unaffected, since they would die on the third attempt
anyway. Requeue clears the story and the flag, so a fixed job starts
its new attempt budget with a clean story.

## Rate limiting

Rate limiting is a sliding window counted in SQLite, not in memory, so
the budget survives a restart. Each claim records one run row with its
start time, and a job is only claimed when fewer than N runs of its
handler started inside the window. The check, the claim, and the run
row happen in one immediate transaction, so two workers racing for the
last unit of budget cannot both win it. A limited job is skipped, not
claimed, so it never blocks other handlers, and old run rows are pruned
when the window is checked. `GET /rate-limits` shows used, remaining,
and seconds until the budget resets.

## Progress and cancellation

Progress is a plain percent and message on the same job row, written by
a reporter the worker hands to handlers that ask for one, so polling
`GET /jobs/{id}` is the progress API and no separate channel is needed.
Cancellation is cooperative, not a thread kill, because killing a thread
mid handler can corrupt shared state. A pending job is cancelled at once,
a running job gets a `cancel_requested` flag its handler checks each
step, and the worker marks it cancelled instead of completed when the
handler returns, so a cancelled job is never retried or dead lettered.

## Recovery on restart

Recovery runs on every startup, because only the claim marks a job
running, and only the worker clears that mark, so a job still running at
startup means its worker died. The crashed attempt counts toward max
attempts, so a job that kills its worker every time is dead lettered
instead of looping forever. A recovered job is reset to pending with its
claim and progress cleared, and a pending cancel request is kept, so the
job still ends as cancelled when it runs again. The crash is recorded in
the job failure story. The last recovery summary is kept on the queue
and served by `GET /recovery`, so a restart is visible, not silent.

## Batch enqueue

A batch is one transaction, not one insert per job, so a client that
enqueues a hundred jobs never leaves a partial batch behind when one
item is bad. Every spec is validated before anything is inserted, and
the whole batch is committed once, so a bad item fails the batch with
zero jobs created. Idempotency works per item, so a key that already
exists returns that job instead of a duplicate, and the same key twice
in one batch returns the same job twice. Batches are capped at 100 jobs,
so one request cannot flood the queue or hold the writer lock for long.

## Dashboard and observability

The dashboard is one read only summary over the jobs table, not a new
store, so it can never drift from the data it reports on. Throughput
reads `finished_at`, a timestamp set when a job reaches a terminal state,
completed, dead, or cancelled, so it is a plain count over the last
minute and the last hour, and requeue clears it, so a job that runs
again is counted only when it finishes again. The newest dead jobs carry
their errors, so the commonest failure is visible without a query.

## Measured performance

Measured on the dev machine, 2026-11-20, with
`python -m queue.loadtest`, 500 jobs, throwaway database, real claim,
run, and finish path, not a mock.

- Trivial sum handler, 4 workers, about 361 jobs per second, 1 worker,
  about 399 jobs per second, extra workers do not help, SQLite
  serialises the writers and the claim is the bottleneck
- Handler that sleeps 50 ms per job, 4 workers, about 67 jobs per
  second, 1 worker, about 19 jobs per second, about 3.6x faster, the
  workers pay off when handlers do real work
- Enqueue, about 30,000 jobs per second in batches of 100
- Mean latency, enqueue to completion, about 0.7 seconds for the 500
  job run, most of it queueing, not handler time

Quote these numbers with the machine and the job count, they are from
one dev machine, not a benchmark.

## Limits of this design

- One SQLite file, one writer at a time, so throughput tops out in the
  low hundreds of jobs per second for trivial handlers, and the claim
  is the bottleneck.
- Workers poll every 0.05 seconds, so a job can wait up to a poll
  interval after becoming due, plus claim contention, and an empty
  queue still burns a little CPU polling.
- A timed out handler keeps running in a background thread, because
  Python cannot kill a thread, so a handler that leaks memory or holds
  a lock keeps leaking after its timeout.
- Handlers run in the worker process, so a handler that crashes the
  process kills the worker, and a handler that blocks the GIL slows
  every other worker in the same process.
- There is no scheduling beyond `run_at`, no cron style recurrence yet,
  and no way to run a handler on a different machine.
- The dashboard counts over the whole jobs table, so it slows down as
  the table grows, and finished jobs are never archived or pruned.

## What would change at larger scale

- Move the store to Postgres, keep the same claim pattern, one atomic
  update with a subquery and `RETURNING`, and add a partial index on
  pending due jobs, so the claim stays fast as the table grows.
- Replace polling with `LISTEN` and `NOTIFY`, or a lightweight wakeup,
  so workers sleep until a job is due instead of polling.
- Run each handler in its own process, or a small pool of processes,
  so a crash or a timeout truly ends the handler, and the GIL stops
  being shared across handlers.
- Split the API and the workers onto separate machines, put the
  database behind a connection pool, and shard by handler or tenant
  when one writer becomes the limit.
- Archive finished jobs out of the hot table, keep the dashboard over
  a rollup, and add alerting on dead letter rate, poison count, and
  oldest pending age, the three numbers that say the queue is stuck.
- Add a scheduler service for recurring jobs, one row per schedule,
  one claim per due tick, so a missed tick is visible instead of
  silently skipped.

## Failure modes

- Worker process dies mid job: the job stays running, recovery on
  startup returns it to pending, or dead letters it when out of
  attempts. The claim is the only thing that marks running, so a
  running job at startup is proof its worker died.
- Handler raises every time: retried with backoff, then dead lettered,
  poison flagged after 3 identical errors. The failure story names the
  error, requeue clears it when fixed.
- Handler hangs: timed out after `timeout_seconds`, failed like any
  other failure. The worker runs handlers in a daemon thread and moves on.
- Handler calls a flaky third party: rate limit caps starts per window,
  backoff spaces retries. The limited job waits, other handlers keep
  running.
- Client retries an enqueue: the same idempotency key returns the
  original job. The key is unique in the database, not a cache.
- API process dies mid batch: zero jobs created, the batch is one
  transaction. A bad item or a crash leaves no partial batch.
- Database file is deleted: everything is lost, there is no backup.
  SQLite is one file, back it up, this is the design's weakest point.
- Two workers race for one job: one wins the atomic claim, the other
  gets nothing. The claim is one statement, not select then update.

## Alternatives considered

- Redis lists for the queue, rejected for now, because persistence
  would need a second store anyway, and SQLite already gives durability,
  queries, and inspection in one file.
- A separate table for delayed jobs, rejected, because `run_at` on the
  same row keeps one claim query and one ordering rule.
- A separate dead letter table, rejected, because dead is a status on
  the same row, so a dead job keeps its full history in one place.
- In memory rate limiting, rejected, because the budget must survive
  a restart, and a SQLite count is cheap at this scale.
- Killing threads on timeout, rejected, because Python cannot do it
  safely, so the timeout fails the job and lets the thread finish in
  the background instead.
