import tempfile
import threading
import time
from pathlib import Path

from queue.jobs import HANDLERS, JobQueue
from queue.workers import Worker, run_workers


def fresh_queue():
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    return JobQueue(db_path=tmp.name)


def test_enqueue_and_process():
    q = fresh_queue()
    job = q.enqueue("sum", {"numbers": [1, 2, 3]})
    assert job["status"] == "pending"
    done = q.process_next()
    assert done["status"] == "completed"
    assert '"total": 6' in done["result"]


def test_retry_then_dead_letter():
    q = fresh_queue()
    job = q.enqueue("nope", {}, max_attempts=2)
    q.process_next()
    assert q.get(job["id"])["status"] == "pending"
    q.process_next()
    assert q.get(job["id"])["status"] == "dead"


def test_idempotency():
    q = fresh_queue()
    a = q.enqueue("echo", {"message": "hi"}, idempotency_key="k1")
    b = q.enqueue("echo", {"message": "hi"}, idempotency_key="k1")
    assert a["id"] == b["id"]
    assert len(q.list_jobs()) == 1


def test_delayed_job_not_processed_early():
    q = fresh_queue()
    job = q.enqueue("sum", {"numbers": [1, 2]}, run_at=2000)
    assert q.process_next(now=1999) is None
    assert q.get(job["id"])["status"] == "pending"


def test_delayed_job_processed_when_due():
    q = fresh_queue()
    job = q.enqueue("sum", {"numbers": [1, 2]}, run_at=1000)
    done = q.process_next(now=1000)
    assert done["id"] == job["id"]
    assert done["status"] == "completed"


def test_delay_seconds_and_due_order():
    q = fresh_queue()
    later = q.enqueue("echo", {"message": "later"}, run_at=2000)
    sooner = q.enqueue("echo", {"message": "sooner"}, run_at=1500)
    assert q.process_next(now=1600)["id"] == sooner["id"]
    assert q.get(later["id"])["status"] == "pending"
    delayed = q.enqueue("sum", {"numbers": [4]}, delay_seconds=3600)
    assert delayed["run_at"] > delayed["created_at"]


def test_fail_once_recovers():
    q = fresh_queue()
    job = q.enqueue("fail_once", {})
    q.process_next()
    assert q.get(job["id"])["status"] == "pending"
    q.process_next()
    assert q.get(job["id"])["status"] == "completed"


def test_priority_jumps_queue():
    q = fresh_queue()
    low = q.enqueue("echo", {"message": "low"}, priority=0)
    high = q.enqueue("echo", {"message": "high"}, priority=10)
    assert q.process_next()["id"] == high["id"]
    assert q.process_next()["id"] == low["id"]


def test_same_priority_falls_back_to_run_at():
    q = fresh_queue()
    later = q.enqueue("echo", {"message": "later"}, run_at=2000, priority=5)
    sooner = q.enqueue("echo", {"message": "sooner"}, run_at=1500, priority=5)
    assert q.process_next(now=2500)["id"] == sooner["id"]
    assert q.process_next(now=2500)["id"] == later["id"]


def test_priority_does_not_beat_delay():
    q = fresh_queue()
    future = q.enqueue("echo", {"message": "future"}, run_at=5000, priority=99)
    due = q.enqueue("echo", {"message": "due"}, run_at=1000, priority=0)
    assert q.process_next(now=2000)["id"] == due["id"]
    assert q.get(future["id"])["status"] == "pending"


def test_default_priority_is_zero():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "plain"})
    assert job["priority"] == 0


def test_claim_records_worker_id():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"})
    done = q.process_next(worker_id="worker-7")
    assert done["id"] == job["id"]
    assert done["claimed_by"] == "worker-7"


def test_atomic_claim_gives_each_job_to_one_worker():
    q = fresh_queue()
    seen = []
    lock = threading.Lock()

    def counting(payload):
        with lock:
            seen.append(payload["n"])
        time.sleep(0.02)
        return {"n": payload["n"]}

    HANDLERS["counting"] = counting
    for n in range(20):
        q.enqueue("counting", {"n": n})

    def drain():
        while q.process_next(worker_id=threading.current_thread().name) is not None:
            pass

    threads = [threading.Thread(target=drain) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(seen) == list(range(20))
    jobs = q.list_jobs()
    assert len(jobs) == 20
    assert all(job["status"] == "completed" for job in jobs)
    assert all(job["attempts"] == 1 for job in jobs)
    assert len({job["claimed_by"] for job in jobs}) > 1


def test_workers_run_jobs_in_parallel():
    q = fresh_queue()
    for _ in range(4):
        q.enqueue("slow", {"seconds": 0.3})
    workers = run_workers(q, 2, poll_interval=0.01)
    start = time.monotonic()
    try:
        while time.monotonic() - start < 5:
            if all(job["status"] == "completed" for job in q.list_jobs()):
                break
            time.sleep(0.05)
    finally:
        for worker in workers:
            worker.stop()
    assert all(job["status"] == "completed" for job in q.list_jobs())
    assert sum(worker.processed for worker in workers) == 4
    assert time.monotonic() - start < 1.0


def test_worker_run_until_empty():
    q = fresh_queue()
    for i in range(3):
        q.enqueue("echo", {"message": str(i)})
    worker = Worker(q, "worker-1")
    worker.run_until_empty()
    assert worker.processed == 3
    assert all(job["status"] == "completed" for job in q.list_jobs())


def test_progress_defaults_to_zero():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"})
    assert job["progress"] == 0
    assert job["cancel_requested"] == 0


def test_progress_reported_by_handler():
    q = fresh_queue()
    job = q.enqueue("progress_demo", {"steps": 4})
    done = q.process_next()
    assert done["id"] == job["id"]
    assert done["status"] == "completed"
    assert done["progress"] == 100
    assert done["progress_message"] == "step 4 of 4"


def test_report_progress_clamps_to_0_100():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"})
    q.report_progress(job["id"], 150, "too far")
    assert q.get(job["id"])["progress"] == 100
    q.report_progress(job["id"], -5)
    assert q.get(job["id"])["progress"] == 0


def test_cancel_pending_job_never_runs():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"})
    cancelled = q.cancel(job["id"])
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancel_requested"] == 1
    assert q.process_next() is None
    assert q.get(job["id"])["status"] == "cancelled"
    assert q.get(job["id"])["attempts"] == 0


def test_cancel_running_job_stops_it_cooperatively():
    q = fresh_queue()
    job = q.enqueue("long", {"steps": 20})
    result = {}

    def run():
        result["done"] = q.process_next()

    thread = threading.Thread(target=run)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            current = q.get(job["id"])
            if current["status"] == "running" and current["progress"] > 0:
                break
            time.sleep(0.02)
        cancelled = q.cancel(job["id"])
        assert cancelled["status"] == "running"
        assert cancelled["cancel_requested"] == 1
    finally:
        thread.join(timeout=5)
    assert not thread.is_alive()
    done = result["done"]
    assert done["status"] == "cancelled"
    assert done["progress"] < 100


def test_cancel_completed_job_is_a_noop():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"})
    q.process_next()
    assert q.cancel(job["id"])["status"] == "completed"


def test_cancel_unknown_job_returns_none():
    q = fresh_queue()
    assert q.cancel(9999) is None


def test_cancel_endpoint():
    import pytest
    from fastapi import HTTPException

    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        job = main.enqueue(main.EnqueueRequest(handler="echo", payload={"message": "hi"}))
        assert job["progress"] == 0
        cancelled = main.cancel_job(job["id"])
        assert cancelled["status"] == "cancelled"
        with pytest.raises(HTTPException):
            main.cancel_job(9999)
        assert main.get_job(job["id"])["status"] == "cancelled"
    finally:
        main.jobs = old_jobs
