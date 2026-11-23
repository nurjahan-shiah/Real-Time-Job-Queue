import tempfile
import threading
import time
from pathlib import Path

from queue.jobs import BACKOFF_STRATEGIES, HANDLERS, MAX_BATCH_SIZE, JobQueue, compute_backoff
from queue.loadtest import format_results, run_load_test
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


def test_dead_letter_listed_with_error():
    q = fresh_queue()
    job = q.enqueue("nope", {}, max_attempts=1)
    q.process_next()
    dead = q.list_dead()
    assert len(dead) == 1
    assert dead[0]["id"] == job["id"]
    assert dead[0]["status"] == "dead"
    assert dead[0]["attempts"] == 1
    assert "unknown handler" in dead[0]["error"]


def test_dead_list_is_empty_when_nothing_died():
    q = fresh_queue()
    q.enqueue("echo", {"message": "hi"})
    assert q.list_dead() == []


def test_requeue_dead_job_runs_again():
    q = fresh_queue()
    calls = {"n": 0}

    def flaky(payload):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RuntimeError("flaky failure")
        return {"ok": True}

    HANDLERS["flaky"] = flaky
    job = q.enqueue("flaky", {}, max_attempts=2)
    q.process_next()
    assert q.get(job["id"])["status"] == "pending"
    q.process_next()
    assert q.get(job["id"])["status"] == "dead"
    again = q.requeue(job["id"])
    assert again["status"] == "pending"
    assert again["attempts"] == 0
    assert again["error"] is None
    assert again["result"] is None
    done = q.process_next()
    assert done["id"] == job["id"]
    assert done["status"] == "completed"
    assert '"ok": true' in done["result"]


def test_requeue_live_job_is_a_noop():
    q = fresh_queue()
    pending = q.enqueue("echo", {"message": "hi"})
    assert q.requeue(pending["id"])["status"] == "pending"
    assert q.get(pending["id"])["attempts"] == 0
    q.process_next()
    assert q.requeue(pending["id"])["status"] == "completed"


def test_requeue_unknown_job_returns_none():
    q = fresh_queue()
    assert q.requeue(9999) is None


def test_dead_and_requeue_endpoints():
    import pytest
    from fastapi import HTTPException

    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        job = main.enqueue(main.EnqueueRequest(handler="nope", payload={}, max_attempts=1))
        main.process_next()
        dead = main.list_dead()
        assert len(dead) == 1
        assert dead[0]["id"] == job["id"]
        again = main.requeue_job(job["id"])
        assert again["status"] == "pending"
        assert main.list_dead() == []
        with pytest.raises(HTTPException):
            main.requeue_job(9999)
    finally:
        main.jobs = old_jobs


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


def crash_job(q, job_id, attempts=0, progress=0.0):
    """Fake a worker crash: mark the job running as if it was claimed and the process died."""
    import sqlite3

    conn = sqlite3.connect(q.db_path, timeout=30.0)
    conn.execute(
        "UPDATE jobs SET status = 'running', attempts = ?, claimed_by = 'worker-1', "
        "progress = ?, progress_message = 'halfway' WHERE id = ?",
        (attempts, progress, job_id),
    )
    conn.commit()
    conn.close()


def test_restart_recovers_crashed_job_to_pending():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"})
    crash_job(q, job["id"], attempts=0, progress=50)
    q2 = JobQueue(db_path=q.db_path)
    recovered = q2.get(job["id"])
    assert recovered["status"] == "pending"
    assert recovered["attempts"] == 1
    assert recovered["claimed_by"] is None
    assert recovered["progress"] == 0
    assert recovered["progress_message"] is None
    assert q2.last_recovery == {"recovered": 1, "dead": 0, "job_ids": [job["id"]]}
    q3 = JobQueue(db_path=q.db_path)
    assert q3.last_recovery == {"recovered": 0, "dead": 0, "job_ids": []}


def test_recovered_job_runs_to_completion():
    q = fresh_queue()
    job = q.enqueue("sum", {"numbers": [2, 3]})
    crash_job(q, job["id"])
    q2 = JobQueue(db_path=q.db_path)
    done = q2.process_next()
    assert done["id"] == job["id"]
    assert done["status"] == "completed"
    assert '"total": 5' in done["result"]
    assert done["attempts"] == 2


def test_restart_dead_letters_job_out_of_attempts():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"}, max_attempts=2)
    crash_job(q, job["id"], attempts=1)
    q2 = JobQueue(db_path=q.db_path)
    dead = q2.get(job["id"])
    assert dead["status"] == "dead"
    assert dead["attempts"] == 2
    assert "worker died" in dead["error"]
    assert q2.last_recovery == {"recovered": 0, "dead": 1, "job_ids": [job["id"]]}
    assert q2.list_dead()[0]["id"] == job["id"]


def test_recovery_leaves_other_jobs_alone():
    q = fresh_queue()
    done_job = q.enqueue("echo", {"message": "done"})
    pending = q.enqueue("echo", {"message": "pending"})
    q.process_next()
    dead_job = q.enqueue("nope", {}, max_attempts=1, run_at=1)
    q.process_next()
    cancelled = q.enqueue("echo", {"message": "cancel"})
    q.cancel(cancelled["id"])
    crashed = q.enqueue("echo", {"message": "crashed"})
    crash_job(q, crashed["id"])
    q2 = JobQueue(db_path=q.db_path)
    assert q2.get(pending["id"])["status"] == "pending"
    assert q2.get(pending["id"])["attempts"] == 0
    assert q2.get(done_job["id"])["status"] == "completed"
    assert q2.get(dead_job["id"])["status"] == "dead"
    assert q2.get(cancelled["id"])["status"] == "cancelled"
    assert q2.get(crashed["id"])["status"] == "pending"
    assert q2.last_recovery["recovered"] == 1
    assert q2.last_recovery["dead"] == 0


def test_no_orphans_means_empty_recovery():
    q = fresh_queue()
    q.enqueue("echo", {"message": "hi"})
    assert q.last_recovery == {"recovered": 0, "dead": 0, "job_ids": []}


def test_recovery_endpoint():
    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        job = q.enqueue("echo", {"message": "hi"})
        crash_job(q, job["id"])
        q2 = JobQueue(db_path=q.db_path)
        main.jobs = q2
        summary = main.recovery()
        assert summary["recovered"] == 1
        assert summary["job_ids"] == [job["id"]]
    finally:
        main.jobs = old_jobs


def test_no_limit_means_unlimited_runs():
    q = fresh_queue()
    for _ in range(5):
        q.enqueue("echo", {"message": "hi"}, run_at=1000)
    for _ in range(5):
        done = q.process_next(now=1000)
        assert done["status"] == "completed"
    assert q.process_next(now=1000) is None


def test_rate_limit_blocks_second_run_in_window():
    q = fresh_queue()
    q.set_rate_limit("echo", 1, 60)
    first = q.enqueue("echo", {"message": "one"}, run_at=1000)
    second = q.enqueue("echo", {"message": "two"}, run_at=1000)
    done = q.process_next(now=1000)
    assert done["id"] == first["id"]
    assert done["status"] == "completed"
    assert q.process_next(now=1030) is None
    assert q.get(second["id"])["status"] == "pending"
    done = q.process_next(now=1061)
    assert done["id"] == second["id"]
    assert done["status"] == "completed"


def test_rate_limit_is_per_handler():
    q = fresh_queue()
    q.set_rate_limit("echo", 1, 60)
    echo_job = q.enqueue("echo", {"message": "hi"}, run_at=1000)
    sum_job = q.enqueue("sum", {"numbers": [1, 2]}, run_at=1000)
    assert q.process_next(now=1000)["id"] == echo_job["id"]
    done = q.process_next(now=1000)
    assert done["id"] == sum_job["id"]
    assert done["status"] == "completed"


def test_limited_job_does_not_block_other_handlers():
    q = fresh_queue()
    q.set_rate_limit("echo", 1, 60)
    first = q.enqueue("echo", {"message": "first"}, run_at=1000)
    q.process_next(now=1000)
    assert q.get(first["id"])["status"] == "completed"
    blocked = q.enqueue("echo", {"message": "blocked"}, run_at=1000, priority=10)
    other = q.enqueue("sum", {"numbers": [1, 2]}, run_at=1000, priority=0)
    done = q.process_next(now=1000)
    assert done["id"] == other["id"]
    assert done["status"] == "completed"
    assert q.get(blocked["id"])["status"] == "pending"


def test_rate_limit_window_slides():
    q = fresh_queue()
    q.set_rate_limit("echo", 2, 10)
    jobs = [q.enqueue("echo", {"message": str(i)}, run_at=1000) for i in range(3)]
    assert q.process_next(now=1000)["id"] == jobs[0]["id"]
    assert q.process_next(now=1001)["id"] == jobs[1]["id"]
    assert q.process_next(now=1002) is None
    done = q.process_next(now=1010)
    assert done["id"] == jobs[2]["id"]
    assert done["status"] == "completed"


def test_set_rate_limit_rejects_bad_values():
    import pytest

    q = fresh_queue()
    with pytest.raises(ValueError):
        q.set_rate_limit("echo", 0, 60)
    with pytest.raises(ValueError):
        q.set_rate_limit("echo", 1, 0)
    with pytest.raises(ValueError):
        q.set_rate_limit("echo", 1, -5)
    assert q.get_rate_limit("echo") is None


def test_rate_limit_status_reports_usage():
    q = fresh_queue()
    assert q.rate_limit_status("echo") is None
    q.set_rate_limit("echo", 2, 60)
    status = q.rate_limit_status("echo", now=1000)
    assert status["used"] == 0
    assert status["remaining"] == 2
    assert status["reset_in"] == 0
    q.enqueue("echo", {"message": "hi"}, run_at=1000)
    q.process_next(now=1000)
    status = q.rate_limit_status("echo", now=1010)
    assert status["used"] == 1
    assert status["remaining"] == 1
    assert status["reset_in"] == 50


def test_clear_rate_limit_frees_the_handler():
    q = fresh_queue()
    q.set_rate_limit("echo", 1, 60)
    first = q.enqueue("echo", {"message": "one"}, run_at=1000)
    second = q.enqueue("echo", {"message": "two"}, run_at=1000)
    q.process_next(now=1000)
    assert q.get(first["id"])["status"] == "completed"
    assert q.process_next(now=1000) is None
    q.clear_rate_limit("echo")
    assert q.get_rate_limit("echo") is None
    done = q.process_next(now=1000)
    assert done["id"] == second["id"]
    assert done["status"] == "completed"


def test_rate_limit_survives_restart():
    q = fresh_queue()
    q.set_rate_limit("echo", 1, 60)
    first = q.enqueue("echo", {"message": "one"}, run_at=1000)
    second = q.enqueue("echo", {"message": "two"}, run_at=1000)
    q.process_next(now=1000)
    assert q.get(first["id"])["status"] == "completed"
    q2 = JobQueue(db_path=q.db_path)
    assert q2.get_rate_limit("echo") == {
        "handler": "echo",
        "max_per_window": 1,
        "window_seconds": 60,
    }
    assert q2.process_next(now=1030) is None
    assert q2.get(second["id"])["status"] == "pending"


def test_rate_limit_endpoints():
    import pytest
    from fastapi import HTTPException

    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        with pytest.raises(HTTPException):
            main.get_rate_limit("echo")
        created = main.set_rate_limit(
            main.RateLimitRequest(handler="echo", max_per_window=1, window_seconds=60)
        )
        assert created["handler"] == "echo"
        assert created["max_per_window"] == 1
        assert created["remaining"] == 1
        limits = main.list_rate_limits()
        assert len(limits) == 1
        assert limits[0]["handler"] == "echo"
        first = main.enqueue(main.EnqueueRequest(handler="echo", payload={"message": "one"}))
        second = main.enqueue(main.EnqueueRequest(handler="echo", payload={"message": "two"}))
        done = main.process_next()
        assert done["id"] == first["id"]
        assert main.process_next() == {"processed": None}
        assert main.get_job(second["id"])["status"] == "pending"
        status = main.get_rate_limit("echo")
        assert status["used"] == 1
        assert status["remaining"] == 0
        assert status["reset_in"] > 0
        with pytest.raises(HTTPException):
            main.set_rate_limit(
                main.RateLimitRequest(handler="echo", max_per_window=0, window_seconds=60)
            )
    finally:
        main.jobs = old_jobs


def test_timeout_seconds_stored_on_enqueue():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"}, timeout_seconds=5)
    assert job["timeout_seconds"] == 5
    plain = q.enqueue("echo", {"message": "hi"})
    assert plain["timeout_seconds"] is None


def test_enqueue_rejects_bad_timeout():
    import pytest

    q = fresh_queue()
    with pytest.raises(ValueError):
        q.enqueue("echo", {"message": "hi"}, timeout_seconds=0)
    with pytest.raises(ValueError):
        q.enqueue("echo", {"message": "hi"}, timeout_seconds=-1)
    assert q.list_jobs() == []


def test_job_completes_within_timeout():
    q = fresh_queue()
    job = q.enqueue("slow", {"seconds": 0.05}, timeout_seconds=5)
    done = q.process_next()
    assert done["id"] == job["id"]
    assert done["status"] == "completed"
    assert done["attempts"] == 1


def test_slow_job_times_out_then_retries():
    q = fresh_queue()
    job = q.enqueue("hang", {"seconds": 0.5}, timeout_seconds=0.1, max_attempts=2)
    done = q.process_next()
    assert done["status"] == "pending"
    assert done["attempts"] == 1
    assert "timed out after 0.1 seconds" in done["error"]
    done = q.process_next()
    assert done["status"] == "dead"
    assert done["attempts"] == 2
    assert "timed out" in done["error"]


def test_timeout_dead_letters_at_max_attempts():
    q = fresh_queue()
    job = q.enqueue("hang", {"seconds": 0.5}, timeout_seconds=0.1, max_attempts=1)
    done = q.process_next()
    assert done["status"] == "dead"
    assert done["attempts"] == 1
    assert "timed out after 0.1 seconds" in done["error"]
    assert q.list_dead()[0]["id"] == job["id"]


def test_job_without_timeout_waits_for_slow_handler():
    q = fresh_queue()
    job = q.enqueue("slow", {"seconds": 0.3})
    done = q.process_next()
    assert done["status"] == "completed"
    assert '"slept": 0.3' in done["result"]


def test_requeued_timed_out_job_keeps_its_timeout():
    q = fresh_queue()
    job = q.enqueue("hang", {"seconds": 0.5}, timeout_seconds=0.1, max_attempts=1)
    q.process_next()
    assert q.get(job["id"])["status"] == "dead"
    again = q.requeue(job["id"])
    assert again["status"] == "pending"
    assert again["timeout_seconds"] == 0.1
    assert again["attempts"] == 0
    done = q.process_next()
    assert done["status"] == "dead"
    assert "timed out" in done["error"]


def test_timeout_endpoint():
    import pytest
    from fastapi import HTTPException

    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        job = main.enqueue(
            main.EnqueueRequest(
                handler="hang", payload={"seconds": 0.5},
                timeout_seconds=0.1, max_attempts=1,
            )
        )
        assert job["timeout_seconds"] == 0.1
        done = main.process_next()
        assert done["status"] == "dead"
        assert "timed out" in done["error"]
        with pytest.raises(HTTPException):
            main.enqueue(
                main.EnqueueRequest(handler="echo", payload={}, timeout_seconds=0)
            )
    finally:
        main.jobs = old_jobs


def test_dashboard_counts_by_status_and_handler():
    q = fresh_queue()
    q.enqueue("echo", {"message": "one"})
    q.enqueue("echo", {"message": "two"})
    q.enqueue("sum", {"numbers": [1]})
    q.enqueue("nope", {}, max_attempts=1)
    for _ in range(4):
        q.process_next()
    stats = q.dashboard_stats()
    assert stats["total_jobs"] == 4
    assert stats["by_status"]["completed"] == 3
    assert stats["by_status"]["dead"] == 1
    assert stats["by_handler"]["echo"] == {
        "total": 2, "completed": 2, "dead": 0, "pending": 0,
        "running": 0, "cancelled": 0,
    }
    assert stats["by_handler"]["sum"]["completed"] == 1
    assert stats["by_handler"]["nope"]["dead"] == 1


def test_dashboard_on_empty_queue():
    q = fresh_queue()
    stats = q.dashboard_stats()
    assert stats["total_jobs"] == 0
    assert stats["by_status"] == {}
    assert stats["by_handler"] == {}
    assert stats["backlog"] == {
        "due_now": 0, "scheduled_future": 0, "oldest_pending_age_seconds": None,
    }
    assert stats["throughput"] == {
        "completed_last_minute": 0, "completed_last_hour": 0, "dead_last_hour": 0,
    }
    assert stats["recent_dead"] == []
    assert stats["rate_limits"] == []
    assert stats["recovery"] == {"recovered": 0, "dead": 0, "job_ids": []}


def test_dashboard_backlog_splits_due_and_future():
    q = fresh_queue()
    now = time.time()
    q.enqueue("echo", {"message": "due"}, run_at=now - 10)
    q.enqueue("echo", {"message": "future"}, run_at=now + 3600)
    stats = q.dashboard_stats()
    assert stats["backlog"]["due_now"] == 1
    assert stats["backlog"]["scheduled_future"] == 1
    age = stats["backlog"]["oldest_pending_age_seconds"]
    assert age is not None and 0 <= age < 60


def test_dashboard_throughput_counts_recent_completions():
    q = fresh_queue()
    q.enqueue("echo", {"message": "one"})
    q.enqueue("echo", {"message": "two"})
    q.process_next()
    q.process_next()
    stats = q.dashboard_stats()
    assert stats["throughput"]["completed_last_minute"] == 2
    assert stats["throughput"]["completed_last_hour"] == 2
    assert stats["throughput"]["dead_last_hour"] == 0


def test_dashboard_recent_dead_lists_failures():
    q = fresh_queue()
    job = q.enqueue("nope", {}, max_attempts=1)
    q.process_next()
    stats = q.dashboard_stats()
    assert len(stats["recent_dead"]) == 1
    assert stats["recent_dead"][0]["id"] == job["id"]
    assert stats["recent_dead"][0]["handler"] == "nope"
    assert "unknown handler" in stats["recent_dead"][0]["error"]
    assert stats["throughput"]["dead_last_hour"] == 1


def test_dashboard_includes_rate_limits_and_recovery():
    q = fresh_queue()
    q.set_rate_limit("echo", 2, 60)
    q.enqueue("echo", {"message": "hi"})
    q.process_next()
    stats = q.dashboard_stats()
    assert len(stats["rate_limits"]) == 1
    assert stats["rate_limits"][0]["handler"] == "echo"
    assert stats["rate_limits"][0]["used"] == 1
    assert stats["rate_limits"][0]["remaining"] == 1
    assert stats["recovery"] == {"recovered": 0, "dead": 0, "job_ids": []}


def test_finished_at_set_on_completion_and_cleared_on_requeue():
    q = fresh_queue()
    job = q.enqueue("nope", {}, max_attempts=1)
    assert q.get(job["id"])["finished_at"] is None
    done = q.process_next()
    assert done["status"] == "dead"
    assert done["finished_at"] is not None
    again = q.requeue(job["id"])
    assert again["status"] == "pending"
    assert again["finished_at"] is None


def test_dashboard_endpoint():
    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        q.enqueue("echo", {"message": "hi"})
        q.process_next()
        stats = main.dashboard()
        assert stats["total_jobs"] == 1
        assert stats["by_status"]["completed"] == 1
        assert stats["throughput"]["completed_last_minute"] == 1
        assert stats["throughput"]["completed_last_hour"] == 1
    finally:
        main.jobs = old_jobs


def test_batch_enqueue_creates_all_jobs():
    q = fresh_queue()
    created = q.enqueue_many([
        {"handler": "echo", "payload": {"message": "one"}},
        {"handler": "sum", "payload": {"numbers": [1, 2, 3]}},
        {"handler": "echo", "payload": {"message": "three"}},
    ])
    assert len(created) == 3
    assert [job["handler"] for job in created] == ["echo", "sum", "echo"]
    assert created[0]["id"] < created[1]["id"] < created[2]["id"]
    assert all(job["status"] == "pending" for job in created)
    assert '"message": "one"' in created[0]["payload"]
    assert len(q.list_jobs()) == 3


def test_batch_enqueue_empty_list_returns_empty():
    q = fresh_queue()
    assert q.enqueue_many([]) == []
    assert q.list_jobs() == []


def test_batch_enqueue_is_atomic_on_bad_spec():
    import pytest

    q = fresh_queue()
    with pytest.raises(ValueError):
        q.enqueue_many([
            {"handler": "echo", "payload": {"message": "one"}},
            {"handler": "echo", "payload": {"message": "bad"}, "timeout_seconds": 0},
            {"handler": "echo", "payload": {"message": "three"}},
        ])
    assert q.list_jobs() == []


def test_batch_enqueue_missing_handler_fails_whole_batch():
    import pytest

    q = fresh_queue()
    with pytest.raises(ValueError):
        q.enqueue_many([
            {"handler": "echo", "payload": {"message": "one"}},
            {"payload": {"message": "no handler"}},
        ])
    assert q.list_jobs() == []


def test_batch_enqueue_respects_idempotency():
    q = fresh_queue()
    existing = q.enqueue("echo", {"message": "first"}, idempotency_key="dup")
    created = q.enqueue_many([
        {"handler": "echo", "payload": {"message": "again"}, "idempotency_key": "dup"},
        {"handler": "echo", "payload": {"message": "new"}, "idempotency_key": "fresh"},
        {"handler": "echo", "payload": {"message": "new again"}, "idempotency_key": "fresh"},
    ])
    assert created[0]["id"] == existing["id"]
    assert '"message": "first"' in created[0]["payload"]
    assert created[1]["id"] == created[2]["id"]
    assert len(q.list_jobs()) == 2


def test_batch_enqueue_rejects_oversized_batch():
    import pytest

    q = fresh_queue()
    specs = [
        {"handler": "echo", "payload": {"message": str(i)}}
        for i in range(MAX_BATCH_SIZE + 1)
    ]
    with pytest.raises(ValueError):
        q.enqueue_many(specs)
    assert q.list_jobs() == []
    created = q.enqueue_many(specs[:MAX_BATCH_SIZE])
    assert len(created) == MAX_BATCH_SIZE


def test_batch_enqueue_accepts_per_item_options():
    q = fresh_queue()
    now = time.time()
    created = q.enqueue_many([
        {"handler": "echo", "payload": {"message": "urgent"}, "priority": 5},
        {"handler": "echo", "payload": {"message": "later"}, "delay_seconds": 60},
        {"handler": "slow", "payload": {"seconds": 0.05}, "timeout_seconds": 5},
    ])
    assert created[0]["priority"] == 5
    assert created[1]["run_at"] > now + 30
    assert created[2]["timeout_seconds"] == 5


def test_batch_enqueue_endpoint():
    import pytest
    from fastapi import HTTPException

    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        created = main.enqueue_batch(
            main.BatchEnqueueRequest(jobs=[
                main.EnqueueRequest(handler="echo", payload={"message": "one"}),
                main.EnqueueRequest(handler="sum", payload={"numbers": [1, 2]}),
            ])
        )
        assert len(created) == 2
        assert created[0]["handler"] == "echo"
        assert created[1]["handler"] == "sum"
        with pytest.raises(HTTPException):
            main.enqueue_batch(
                main.BatchEnqueueRequest(jobs=[
                    main.EnqueueRequest(handler="echo", payload={}),
                    main.EnqueueRequest(handler="echo", payload={}, timeout_seconds=0),
                ])
            )
        assert len(q.list_jobs()) == 2
        assert main.enqueue_batch(main.BatchEnqueueRequest(jobs=[])) == []
    finally:
        main.jobs = old_jobs


def test_compute_backoff_exponential_doubles_each_attempt():
    assert compute_backoff("exponential", 1.0, 1, 60.0) == 1.0
    assert compute_backoff("exponential", 1.0, 2, 60.0) == 2.0
    assert compute_backoff("exponential", 1.0, 3, 60.0) == 4.0
    assert compute_backoff("exponential", 1.0, 4, 60.0) == 8.0
    assert compute_backoff("exponential", 2.0, 3, 60.0) == 8.0


def test_compute_backoff_capped_at_cap_seconds():
    assert compute_backoff("exponential", 10.0, 5, 60.0) == 60.0
    assert compute_backoff("linear", 10.0, 9, 60.0) == 60.0
    assert compute_backoff("constant", 100.0, 1, 60.0) == 60.0


def test_compute_backoff_linear_constant_and_none():
    assert compute_backoff("linear", 5.0, 1, 60.0) == 5.0
    assert compute_backoff("linear", 5.0, 3, 60.0) == 15.0
    assert compute_backoff("constant", 5.0, 3, 60.0) == 5.0
    assert compute_backoff("none", 5.0, 3, 60.0) == 0.0
    assert set(BACKOFF_STRATEGIES) == {"none", "constant", "linear", "exponential"}


def test_compute_backoff_rejects_bad_settings():
    import pytest

    with pytest.raises(ValueError):
        compute_backoff("random", 1.0, 1, 60.0)
    with pytest.raises(ValueError):
        compute_backoff("exponential", 0, 1, 60.0)
    with pytest.raises(ValueError):
        compute_backoff("exponential", 1.0, 1, 0)
    with pytest.raises(ValueError):
        compute_backoff("exponential", 1.0, 0, 60.0)


def test_failed_job_waits_for_exponential_backoff():
    q = fresh_queue()
    job = q.enqueue("nope", {}, max_attempts=3, run_at=1000,
                    backoff_strategy="exponential", backoff_base_seconds=100.0,
                    backoff_cap_seconds=1000.0)
    first = q.process_next(now=1000)
    assert first["status"] == "pending"
    assert first["attempts"] == 1
    assert first["run_at"] == 1100.0
    assert q.process_next(now=1050) is None
    assert q.get(job["id"])["status"] == "pending"
    second = q.process_next(now=1100)
    assert second["status"] == "pending"
    assert second["attempts"] == 2
    assert second["run_at"] == 1300.0
    third = q.process_next(now=1300)
    assert third["status"] == "dead"
    assert third["attempts"] == 3


def test_linear_backoff_grows_by_base_each_attempt():
    q = fresh_queue()
    job = q.enqueue("nope", {}, max_attempts=3, run_at=1000,
                    backoff_strategy="linear",
                    backoff_base_seconds=10.0, backoff_cap_seconds=1000.0)
    first = q.process_next(now=1000)
    assert first["run_at"] == 1010.0
    second = q.process_next(now=1010)
    assert second["run_at"] == 1030.0
    assert q.get(job["id"])["status"] == "pending"


def test_backoff_default_is_none_so_retries_run_at_once():
    q = fresh_queue()
    job = q.enqueue("nope", {}, max_attempts=2)
    q.process_next()
    assert q.get(job["id"])["status"] == "pending"
    q.process_next()
    assert q.get(job["id"])["status"] == "dead"


def test_enqueue_rejects_bad_backoff():
    import pytest

    q = fresh_queue()
    with pytest.raises(ValueError):
        q.enqueue("echo", {}, backoff_strategy="random")
    with pytest.raises(ValueError):
        q.enqueue("echo", {}, backoff_base_seconds=0)
    with pytest.raises(ValueError):
        q.enqueue("echo", {}, backoff_cap_seconds=-1)
    assert len(q.list_jobs()) == 0


def test_backoff_settings_stored_on_job_row():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"}, backoff_strategy="linear",
                    backoff_base_seconds=2.5, backoff_cap_seconds=30.0)
    assert job["backoff_strategy"] == "linear"
    assert job["backoff_base_seconds"] == 2.5
    assert job["backoff_cap_seconds"] == 30.0
    plain = q.enqueue("echo", {"message": "plain"})
    assert plain["backoff_strategy"] == "none"
    assert plain["backoff_base_seconds"] == 1.0
    assert plain["backoff_cap_seconds"] == 60.0


def test_backoff_endpoint():
    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        job = main.enqueue(main.EnqueueRequest(
            handler="echo", payload={"message": "hi"},
            backoff_strategy="exponential", backoff_base_seconds=2.0,
            backoff_cap_seconds=30.0,
        ))
        assert job["backoff_strategy"] == "exponential"
        assert job["backoff_base_seconds"] == 2.0
        assert job["backoff_cap_seconds"] == 30.0
    finally:
        main.jobs = old_jobs


def test_batch_enqueue_accepts_backoff_options():
    q = fresh_queue()
    created = q.enqueue_many([
        {"handler": "echo", "payload": {"message": "a"},
         "backoff_strategy": "exponential", "backoff_base_seconds": 3.0},
        {"handler": "echo", "payload": {"message": "b"}},
    ])
    assert created[0]["backoff_strategy"] == "exponential"
    assert created[0]["backoff_base_seconds"] == 3.0
    assert created[1]["backoff_strategy"] == "none"


def test_dashboard_counts_backoff_waiting_job_as_scheduled():
    q = fresh_queue()
    now = time.time()
    q.enqueue("nope", {}, max_attempts=3, backoff_strategy="exponential",
              backoff_base_seconds=3600.0, backoff_cap_seconds=10000.0)
    q.process_next(now=now)
    stats = q.dashboard_stats(now=now)
    assert stats["backlog"]["due_now"] == 0
    assert stats["backlog"]["scheduled_future"] == 1


def test_failure_story_records_each_attempt():
    q = fresh_queue()
    job = q.enqueue("nope", {}, max_attempts=2)
    q.process_next()
    q.process_next()
    failures = q.list_failures(job["id"])
    assert len(failures) == 2
    assert [f["attempt"] for f in failures] == [1, 2]
    assert all("unknown handler" in f["error"] for f in failures)
    assert all(f["failed_at"] > 0 for f in failures)
    assert q.get(job["id"])["status"] == "dead"


def test_poison_job_dead_lettered_early():
    q = fresh_queue()

    def always_broken(payload):
        raise RuntimeError("always broken")

    HANDLERS["always_broken"] = always_broken
    job = q.enqueue("always_broken", {}, max_attempts=5)
    q.process_next()
    assert q.get(job["id"])["status"] == "pending"
    q.process_next()
    assert q.get(job["id"])["status"] == "pending"
    done = q.process_next()
    assert done["status"] == "dead"
    assert done["attempts"] == 3
    assert done["poison"] == 1
    assert "poison job" in done["error"]
    assert "always broken" in done["error"]
    assert q.process_next() is None
    assert q.get(job["id"])["status"] == "dead"


def test_changing_errors_are_not_poison():
    q = fresh_queue()
    calls = {"n": 0}

    def changing(payload):
        calls["n"] += 1
        raise RuntimeError(f"failure number {calls['n']}")

    HANDLERS["changing"] = changing
    job = q.enqueue("changing", {}, max_attempts=5)
    for _ in range(3):
        q.process_next()
    mid = q.get(job["id"])
    assert mid["status"] == "pending"
    assert mid["attempts"] == 3
    assert mid["poison"] == 0
    q.process_next()
    q.process_next()
    done = q.get(job["id"])
    assert done["status"] == "dead"
    assert done["poison"] == 0
    assert len(q.list_failures(job["id"])) == 5


def test_poison_after_mixed_errors():
    q = fresh_queue()
    errors = ["first error", "second error", "second error", "second error"]
    calls = {"n": 0}

    def mixed(payload):
        error = errors[calls["n"]]
        calls["n"] += 1
        raise RuntimeError(error)

    HANDLERS["mixed"] = mixed
    job = q.enqueue("mixed", {}, max_attempts=5)
    for _ in range(3):
        q.process_next()
    assert q.get(job["id"])["status"] == "pending"
    done = q.process_next()
    assert done["status"] == "dead"
    assert done["poison"] == 1
    assert "second error" in done["error"]


def test_timeout_failures_are_poison():
    q = fresh_queue()
    job = q.enqueue("hang", {"seconds": 0.5}, timeout_seconds=0.05, max_attempts=5)
    q.process_next()
    q.process_next()
    done = q.process_next()
    assert done["status"] == "dead"
    assert done["attempts"] == 3
    assert done["poison"] == 1
    assert "timed out" in done["error"]
    failures = q.list_failures(job["id"])
    assert len(failures) == 3
    assert all("timed out" in f["error"] for f in failures)


def test_poison_skips_remaining_backoff():
    q = fresh_queue()

    def always_broken(payload):
        raise RuntimeError("always broken")

    HANDLERS["always_broken"] = always_broken
    job = q.enqueue("always_broken", {}, max_attempts=5, run_at=1000,
                    backoff_strategy="exponential", backoff_base_seconds=100.0,
                    backoff_cap_seconds=1000.0)
    q.process_next(now=1000)
    q.process_next(now=1100)
    done = q.process_next(now=1300)
    assert done["status"] == "dead"
    assert done["attempts"] == 3
    assert done["poison"] == 1


def test_requeue_clears_failure_story_and_poison():
    q = fresh_queue()

    def always_broken(payload):
        raise RuntimeError("always broken")

    HANDLERS["always_broken"] = always_broken
    job = q.enqueue("always_broken", {}, max_attempts=5)
    for _ in range(3):
        q.process_next()
    dead = q.get(job["id"])
    assert dead["poison"] == 1
    assert len(q.list_failures(job["id"])) == 3
    again = q.requeue(job["id"])
    assert again["status"] == "pending"
    assert again["poison"] == 0
    assert again["attempts"] == 0
    assert q.list_failures(job["id"]) == []


def test_healthy_job_has_empty_failure_story():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"})
    q.process_next()
    assert q.list_failures(job["id"]) == []


def test_failures_endpoint():
    import pytest
    from fastapi import HTTPException

    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        job = main.enqueue(main.EnqueueRequest(handler="nope", payload={}, max_attempts=1))
        main.process_next()
        story = main.job_failures(job["id"])
        assert story["job_id"] == job["id"]
        assert story["failure_count"] == 1
        assert len(story["failures"]) == 1
        assert "unknown handler" in story["failures"][0]["error"]
        assert story["distinct_errors"] == ["unknown handler: nope"]
        assert story["poison"] is False
        with pytest.raises(HTTPException):
            main.job_failures(9999)
    finally:
        main.jobs = old_jobs


def test_poison_endpoint():
    import app.main as main

    q = fresh_queue()
    old_jobs = main.jobs
    main.jobs = q
    try:
        assert main.list_poison() == []

        def always_broken(payload):
            raise RuntimeError("always broken")

        HANDLERS["always_broken"] = always_broken
        job = q.enqueue("always_broken", {}, max_attempts=5)
        for _ in range(3):
            q.process_next()
        poison = main.list_poison()
        assert len(poison) == 1
        assert poison[0]["id"] == job["id"]
        assert poison[0]["poison"] == 1
    finally:
        main.jobs = old_jobs


def test_failure_story_includes_worker_crash():
    q = fresh_queue()
    job = q.enqueue("echo", {"message": "hi"})
    crash_job(q, job["id"], attempts=0, progress=50)
    q2 = JobQueue(db_path=q.db_path)
    failures = q2.list_failures(job["id"])
    assert len(failures) == 1
    assert failures[0]["attempt"] == 1
    assert "worker died" in failures[0]["error"]
    assert q2.get(job["id"])["status"] == "pending"


def test_dashboard_counts_poison_jobs():
    q = fresh_queue()

    def always_broken(payload):
        raise RuntimeError("always broken")

    HANDLERS["always_broken"] = always_broken
    job = q.enqueue("always_broken", {}, max_attempts=5)
    for _ in range(3):
        q.process_next()
    stats = q.dashboard_stats()
    assert stats["poison_jobs"] == 1
    assert len(stats["recent_dead"]) == 1
    assert stats["recent_dead"][0]["poison"] == 1
    assert "poison job" in stats["recent_dead"][0]["error"]


def test_load_test_completes_all_jobs():
    results = run_load_test(n=40, workers=4)
    assert results["jobs_enqueued"] == 40
    assert results["workers"] == 4
    assert results["completed"] == 40
    assert results["dead"] == 0
    assert results["cancelled"] == 0
    assert results["timed_out"] is False
    assert results["elapsed_seconds"] > 0
    assert results["jobs_per_second"] > 0
    assert sum(results["per_worker"].values()) == 40
    assert set(results["per_worker"]).issubset(
        {f"worker-{i}" for i in range(1, 5)}
    )


def test_load_test_reports_enqueue_throughput():
    results = run_load_test(n=40, workers=2)
    assert results["enqueue_seconds"] > 0
    assert results["enqueue_jobs_per_second"] > 0
    assert results["completed"] == 40


def test_load_test_mean_latency_is_measured():
    results = run_load_test(n=20, workers=2)
    assert results["completed"] == 20
    assert results["mean_latency_seconds"] is not None
    assert results["mean_latency_seconds"] >= 0
    assert results["mean_latency_seconds"] <= results["elapsed_seconds"] + 1


def test_load_test_counts_dead_jobs():
    results = run_load_test(n=6, workers=2, handler="nope")
    assert results["completed"] == 0
    assert results["dead"] == 6
    assert results["timed_out"] is False
    assert results["mean_latency_seconds"] is None


def test_load_test_rejects_bad_counts():
    import pytest

    with pytest.raises(ValueError):
        run_load_test(n=0, workers=2)
    with pytest.raises(ValueError):
        run_load_test(n=10, workers=0)


def test_load_test_format_results():
    results = run_load_test(n=10, workers=1)
    text = format_results(results)
    assert "10 jobs" in text
    assert "1 workers" in text
    assert "jobs per second" in text
    assert "Completed 10" in text
    assert "Mean latency" in text
    assert "Jobs per worker" in text
    assert "Timed out" not in text


def test_load_test_cli_runs(capsys):
    from queue.loadtest import main

    assert main(["20", "2"]) == 0
    out = capsys.readouterr().out
    assert "20 jobs" in out
    assert "Completed 20" in out
