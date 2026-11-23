"""Load test the queue and report real throughput numbers.

Enqueue N jobs, run W workers against them, and measure how fast the queue
drains: enqueue throughput, processing throughput in jobs per second, mean
time from enqueue to completion, and how the work split across workers.
Run it from the command line with:
python -m queue.loadtest [jobs] [workers]
The numbers it prints are from this machine, not a benchmark, so record
them with the machine and the job count when you quote them.
"""
import sys
import tempfile
import time

from queue.jobs import MAX_BATCH_SIZE, JobQueue
from queue.workers import run_workers

DEFAULT_JOBS = 500
DEFAULT_WORKERS = 4
DEFAULT_HANDLER = "sum"
DEFAULT_PAYLOAD = {"numbers": [1, 2, 3]}
DEFAULT_TIMEOUT_SECONDS = 120.0


def run_load_test(n=DEFAULT_JOBS, workers=DEFAULT_WORKERS,
                  handler=DEFAULT_HANDLER, payload=None, max_attempts=3,
                  db_path=None, timeout_seconds=DEFAULT_TIMEOUT_SECONDS):
    """Enqueue n jobs, drain them with workers, return measured numbers.

    Returns a dict with the job count, worker count, handler, enqueue time
    and rate, processing time and rate in jobs per second, completed, dead,
    and cancelled counts, mean latency from enqueue to completion for
    completed jobs, and a per worker count of jobs claimed. The queue uses a
    throwaway database unless db_path is given, so a load test never touches
    the real jobs.db. When the timeout runs out before the queue drains, the
    workers are stopped and timed_out is True, so a slow run still reports
    what it finished instead of hanging.
    """
    if n < 1:
        raise ValueError("n must be at least 1")
    if workers < 1:
        raise ValueError("workers must be at least 1")
    if payload is None:
        payload = DEFAULT_PAYLOAD
    if db_path is None:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        tmp.close()
        db_path = tmp.name
    queue = JobQueue(db_path=db_path)
    specs = [
        {"handler": handler, "payload": payload, "max_attempts": max_attempts}
        for _ in range(n)
    ]
    enqueue_started = time.time()
    for start in range(0, n, MAX_BATCH_SIZE):
        queue.enqueue_many(specs[start:start + MAX_BATCH_SIZE])
    enqueue_seconds = time.time() - enqueue_started
    started = time.time()
    worker_list = run_workers(queue, workers)
    timed_out = False
    try:
        while True:
            counts = queue.stats()
            terminal = (
                counts.get("completed", 0)
                + counts.get("dead", 0)
                + counts.get("cancelled", 0)
            )
            if terminal >= n:
                break
            if time.time() - started > timeout_seconds:
                timed_out = True
                break
            time.sleep(0.05)
    finally:
        for worker in worker_list:
            worker.stop()
    elapsed_seconds = time.time() - started
    counts = queue.stats()
    completed = counts.get("completed", 0)
    latencies = []
    per_worker = {}
    for job in queue.list_jobs(limit=n):
        claimed_by = job.get("claimed_by")
        if claimed_by:
            per_worker[claimed_by] = per_worker.get(claimed_by, 0) + 1
        if job["status"] == "completed" and job.get("finished_at") is not None:
            latencies.append(job["finished_at"] - job["created_at"])
    mean_latency = (
        sum(latencies) / len(latencies) if latencies else None
    )
    return {
        "jobs_enqueued": n,
        "workers": workers,
        "handler": handler,
        "enqueue_seconds": round(enqueue_seconds, 3),
        "enqueue_jobs_per_second": round(n / enqueue_seconds, 1)
        if enqueue_seconds > 0 else None,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "jobs_per_second": round(n / elapsed_seconds, 1)
        if elapsed_seconds > 0 else None,
        "completed": completed,
        "dead": counts.get("dead", 0),
        "cancelled": counts.get("cancelled", 0),
        "mean_latency_seconds": (
            round(mean_latency, 3) if mean_latency is not None else None
        ),
        "per_worker": per_worker,
        "timed_out": timed_out,
    }


def format_results(results):
    """One readable summary of a load test run, for the terminal and the README."""
    lines = [
        f"Load test, {results['jobs_enqueued']} jobs, "
        f"{results['workers']} workers, handler {results['handler']}",
        f"Enqueue, {results['enqueue_jobs_per_second']} jobs per second "
        f"in {results['enqueue_seconds']} seconds",
        f"Processing, {results['jobs_per_second']} jobs per second "
        f"in {results['elapsed_seconds']} seconds",
        f"Completed {results['completed']}, dead {results['dead']}, "
        f"cancelled {results['cancelled']}",
        f"Mean latency, enqueue to completion, "
        f"{results['mean_latency_seconds']} seconds",
        "Jobs per worker, "
        + ", ".join(
            f"{name} {count}"
            for name, count in sorted(results["per_worker"].items())
        ),
    ]
    if results["timed_out"]:
        lines.append("Timed out before the queue drained")
    return "\n".join(lines)


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    n = int(args[0]) if len(args) > 0 else DEFAULT_JOBS
    workers = int(args[1]) if len(args) > 1 else DEFAULT_WORKERS
    results = run_load_test(n=n, workers=workers)
    print(format_results(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
