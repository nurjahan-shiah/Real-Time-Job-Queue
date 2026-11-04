import tempfile
from pathlib import Path

from queue.jobs import JobQueue


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
