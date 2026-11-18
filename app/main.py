"""HTTP API around the job queue, including delayed jobs, priority, progress, cancellation, dead letter requeue, restart recovery, per handler rate limiting, job timeouts, dashboard stats, batch enqueue, and backoff strategies."""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from queue.jobs import (
    DEFAULT_BACKOFF_BASE_SECONDS,
    DEFAULT_BACKOFF_CAP_SECONDS,
    JobQueue,
)

app = FastAPI(title="Real Time Job Queue")
jobs = JobQueue()


class EnqueueRequest(BaseModel):
    handler: str
    payload: dict = {}
    max_attempts: int = 3
    idempotency_key: str | None = None
    run_at: float | None = None
    delay_seconds: float | None = None
    priority: int = 0
    timeout_seconds: float | None = None
    backoff_strategy: str = "none"
    backoff_base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS
    backoff_cap_seconds: float = DEFAULT_BACKOFF_CAP_SECONDS


@app.post("/jobs")
def enqueue(req: EnqueueRequest):
    try:
        return jobs.enqueue(
            req.handler, req.payload, req.max_attempts, req.idempotency_key,
            run_at=req.run_at, delay_seconds=req.delay_seconds,
            priority=req.priority, timeout_seconds=req.timeout_seconds,
            backoff_strategy=req.backoff_strategy,
            backoff_base_seconds=req.backoff_base_seconds,
            backoff_cap_seconds=req.backoff_cap_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


class BatchEnqueueRequest(BaseModel):
    jobs: list[EnqueueRequest] = []


@app.post("/jobs/batch")
def enqueue_batch(req: BatchEnqueueRequest):
    try:
        return jobs.enqueue_many([spec.model_dump() for spec in req.jobs])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/jobs/{job_id}")
def get_job(job_id: int):
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: int):
    job = jobs.cancel(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.post("/jobs/{job_id}/requeue")
def requeue_job(job_id: int):
    job = jobs.requeue(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/dead")
def list_dead():
    return jobs.list_dead()


@app.get("/jobs")
def list_jobs():
    return jobs.list_jobs()


@app.get("/stats")
def stats():
    return jobs.stats()


@app.get("/dashboard")
def dashboard():
    return jobs.dashboard_stats()


class RateLimitRequest(BaseModel):
    handler: str
    max_per_window: int
    window_seconds: float


@app.post("/rate-limits")
def set_rate_limit(req: RateLimitRequest):
    try:
        jobs.set_rate_limit(req.handler, req.max_per_window, req.window_seconds)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return jobs.rate_limit_status(req.handler)


@app.get("/rate-limits")
def list_rate_limits():
    return [
        jobs.rate_limit_status(limit["handler"])
        for limit in jobs.list_rate_limits()
    ]


@app.get("/rate-limits/{handler}")
def get_rate_limit(handler: str):
    status = jobs.rate_limit_status(handler)
    if status is None:
        raise HTTPException(status_code=404, detail="No rate limit for this handler")
    return status


@app.get("/recovery")
def recovery():
    return jobs.last_recovery


@app.post("/process-next")
def process_next():
    job = jobs.process_next()
    return job if job else {"processed": None}
