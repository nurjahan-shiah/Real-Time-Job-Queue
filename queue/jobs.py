"""SQLite backed job queue with retry, dead letter, idempotency, delayed jobs, priority, multiple workers, progress, cancellation, dead letter inspection, requeue, restart recovery, and per handler rate limiting."""
import inspect
import sqlite3
import time
from pathlib import Path

HANDLERS = {}


def handler(name):
    def wrap(fn):
        HANDLERS[name] = fn
        return fn
    return wrap


@handler("echo")
def echo_handler(payload):
    return {"echo": payload.get("message")}


@handler("sum")
def sum_handler(payload):
    return {"total": sum(payload.get("numbers", []))}


@handler("fail_once")
def fail_once_handler(payload):
    if not getattr(fail_once_handler, "_failed", False):
        fail_once_handler._failed = True
        raise RuntimeError("first attempt fails")
    return {"ok": True}


@handler("slow")
def slow_handler(payload):
    seconds = payload.get("seconds", 0.2)
    time.sleep(seconds)
    return {"slept": seconds}


@handler("progress_demo")
def progress_demo_handler(payload, progress):
    """Report progress in steps, so clients can poll GET /jobs/{id}."""
    steps = payload.get("steps", 4)
    for step in range(1, steps + 1):
        progress(100.0 * step / steps, f"step {step} of {steps}")
    return {"steps": steps}


@handler("long")
def long_handler(payload, progress):
    """Cooperatively stoppable job, it checks for cancellation each step."""
    steps = payload.get("steps", 10)
    for step in range(1, steps + 1):
        if progress.cancelled:
            return {"cancelled": True, "stopped_at_step": step - 1}
        time.sleep(0.05)
        progress(100.0 * step / steps, f"step {step} of {steps}")
    return {"steps": steps}


class ProgressReporter:
    """What a handler gets to report progress and check for cancellation.

    Call it with a percent from 0 to 100 and an optional message, it writes
    both to the job row, so GET /jobs/{id} shows live progress. Read
    .cancelled to stop work early when a client asked to cancel the job.
    """

    def __init__(self, queue, job_id):
        self._queue = queue
        self.job_id = job_id

    def __call__(self, percent, message=None):
        self._queue.report_progress(self.job_id, percent, message)

    @property
    def cancelled(self):
        return self._queue.is_cancel_requested(self.job_id)


class JobQueue:
    def __init__(self, db_path=None):
        self.db_path = Path(db_path) if db_path else Path(__file__).parent / "jobs.db"
        conn = self._conn()
        conn.execute(
            "CREATE TABLE IF NOT EXISTS jobs ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "handler TEXT NOT NULL, payload TEXT NOT NULL, "
            "status TEXT NOT NULL DEFAULT 'pending', "
            "attempts INTEGER NOT NULL DEFAULT 0, "
            "max_attempts INTEGER NOT NULL DEFAULT 3, "
            "idempotency_key TEXT UNIQUE, result TEXT, error TEXT, "
            "created_at REAL NOT NULL, run_at REAL NOT NULL DEFAULT 0, "
            "priority INTEGER NOT NULL DEFAULT 0, claimed_by TEXT, "
            "progress REAL NOT NULL DEFAULT 0, progress_message TEXT, "
            "cancel_requested INTEGER NOT NULL DEFAULT 0)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rate_limits ("
            "handler TEXT PRIMARY KEY, "
            "max_per_window INTEGER NOT NULL, "
            "window_seconds REAL NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rate_limit_runs ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "handler TEXT NOT NULL, started_at REAL NOT NULL)"
        )
        columns = [r[1] for r in conn.execute("PRAGMA table_info(jobs)")]
        if "run_at" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN run_at REAL NOT NULL DEFAULT 0")
        if "priority" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
        if "claimed_by" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN claimed_by TEXT")
        if "progress" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN progress REAL NOT NULL DEFAULT 0")
        if "progress_message" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN progress_message TEXT")
        if "cancel_requested" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        conn.close()
        self.last_recovery = self.recover_orphans()

    def recover_orphans(self):
        """Recover jobs left running by a worker that died, on startup.

        A job is only ever marked running by the atomic claim, and the worker
        marks it completed, pending, dead, or cancelled when the handler
        returns. So a job still marked running when a new JobQueue opens means
        its worker died mid run, the process crashed or was killed, and the
        job would otherwise sit in running forever, because only pending jobs
        are claimed. Recovery runs on every startup, so a restart picks the
        job up again instead of losing it.

        The crashed attempt counts, attempts goes up by one, so a job that
        crashes its worker every time still reaches its max attempts and is
        dead lettered instead of looping forever. A recovered job goes back
        to pending with its claim and progress cleared, so it runs again
        promptly. A job already out of attempts is dead lettered with an
        error saying the worker died while it was running. A cancel request
        is kept, so a job the user asked to cancel still ends as cancelled
        when it runs again.
        """
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status = 'running'"
        ).fetchall()
        recovered = []
        dead = []
        for row in rows:
            job = dict(row)
            attempts = job["attempts"] + 1
            if attempts >= job["max_attempts"]:
                conn.execute(
                    "UPDATE jobs SET status = 'dead', error = ?, attempts = ?, "
                    "claimed_by = NULL WHERE id = ?",
                    ("worker died while job was running, recovered on restart",
                     attempts, job["id"]),
                )
                dead.append(job["id"])
            else:
                conn.execute(
                    "UPDATE jobs SET status = 'pending', attempts = ?, "
                    "claimed_by = NULL, progress = 0, progress_message = NULL "
                    "WHERE id = ?",
                    (attempts, job["id"]),
                )
                recovered.append(job["id"])
        conn.commit()
        conn.close()
        return {"recovered": len(recovered), "dead": len(dead),
                "job_ids": recovered + dead}

    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def enqueue(self, handler_name, payload, max_attempts=3, idempotency_key=None,
                run_at=None, delay_seconds=None, priority=0):
        import json

        conn = self._conn()
        if idempotency_key:
            row = conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row:
                conn.close()
                return dict(row)
        now = time.time()
        if delay_seconds is not None:
            effective_run_at = now + delay_seconds
        elif run_at is not None:
            effective_run_at = run_at
        else:
            effective_run_at = now
        cur = conn.execute(
            "INSERT INTO jobs (handler, payload, max_attempts, idempotency_key, created_at, run_at, priority)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (handler_name, json.dumps(payload), max_attempts, idempotency_key, now,
             effective_run_at, priority),
        )
        job_id = cur.lastrowid
        conn.commit()
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        return dict(row)

    def get(self, job_id):
        conn = self._conn()
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def list_jobs(self, limit=50):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def stats(self):
        conn = self._conn()
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"
        ).fetchall()
        conn.close()
        return {r["status"]: r["n"] for r in rows}

    def report_progress(self, job_id, percent, message=None):
        """Store progress, clamped to 0 to 100, on the job row."""
        percent = max(0.0, min(100.0, float(percent)))
        conn = self._conn()
        conn.execute(
            "UPDATE jobs SET progress = ?, progress_message = ? WHERE id = ?",
            (percent, message, job_id),
        )
        conn.commit()
        conn.close()

    def is_cancel_requested(self, job_id):
        job = self.get(job_id)
        return bool(job and job.get("cancel_requested"))

    def cancel(self, job_id):
        """Cancel a job. Returns the job, or None when it does not exist.

        A pending job is cancelled at once and never runs. A running job
        cannot be killed safely mid handler, so it is flagged instead: the
        handler can check the flag and stop early, and the worker marks the
        job cancelled instead of completed when the handler returns. A job
        that is already completed, dead, or cancelled is left as it is.
        """
        job = self.get(job_id)
        if job is None:
            return None
        if job["status"] in ("completed", "dead", "cancelled"):
            return job
        conn = self._conn()
        if job["status"] == "pending":
            conn.execute(
                "UPDATE jobs SET status = 'cancelled', cancel_requested = 1 WHERE id = ?",
                (job_id,),
            )
        else:
            conn.execute(
                "UPDATE jobs SET cancel_requested = 1 WHERE id = ?",
                (job_id,),
            )
        conn.commit()
        conn.close()
        return self.get(job_id)

    def list_dead(self, limit=50):
        """List dead letter jobs, newest first, with their last error.

        A dead job is one that used up its attempts, so this is the
        inspection view: handler, payload, attempts, and the error that
        killed it, so you can decide whether it is worth requeueing.
        """
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status = 'dead' ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def requeue(self, job_id):
        """Put a dead job back in the queue. Returns the job, or None when it does not exist.

        Only dead jobs are requeued. A job in any other status is returned
        unchanged, so requeueing a live job can never duplicate work. The
        requeued job keeps its handler, payload, priority, and idempotency
        key, but attempts, error, result, and progress are cleared, and
        run_at is set to now, so it runs again promptly with a fresh
        attempt budget.
        """
        job = self.get(job_id)
        if job is None:
            return None
        if job["status"] != "dead":
            return job
        conn = self._conn()
        conn.execute(
            "UPDATE jobs SET status = 'pending', attempts = 0, error = NULL, "
            "result = NULL, progress = 0, progress_message = NULL, "
            "cancel_requested = 0, claimed_by = NULL, run_at = ? WHERE id = ?",
            (time.time(), job_id),
        )
        conn.commit()
        conn.close()
        return self.get(job_id)

    def set_rate_limit(self, handler_name, max_per_window, window_seconds):
        """Cap how often one handler runs, at most max_per_window starts in any rolling window of window_seconds.

        The limit lives in SQLite, so it survives a restart, and the runs
        counted against it are stored too, so a restart does not reset the
        budget. A limit below 1 or a window that is not positive makes no
        sense, so both are rejected.
        """
        if max_per_window < 1:
            raise ValueError("max_per_window must be at least 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        conn = self._conn()
        conn.execute(
            "INSERT INTO rate_limits (handler, max_per_window, window_seconds) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(handler) DO UPDATE SET max_per_window = excluded.max_per_window, "
            "window_seconds = excluded.window_seconds",
            (handler_name, max_per_window, window_seconds),
        )
        conn.commit()
        conn.close()
        return self.get_rate_limit(handler_name)

    def get_rate_limit(self, handler_name):
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM rate_limits WHERE handler = ?", (handler_name,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def list_rate_limits(self):
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM rate_limits ORDER BY handler"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def clear_rate_limit(self, handler_name):
        """Remove a handler's limit, and drop its recorded runs."""
        conn = self._conn()
        conn.execute(
            "DELETE FROM rate_limits WHERE handler = ?", (handler_name,)
        )
        conn.execute(
            "DELETE FROM rate_limit_runs WHERE handler = ?", (handler_name,)
        )
        conn.commit()
        conn.close()

    def rate_limit_status(self, handler_name, now=None):
        """How much of a handler's rate budget is used, or None when it has no limit.

        used counts starts in the current rolling window, remaining is what
        is left, and reset_in is seconds until the oldest start in the window
        ages out, or 0 when nothing is running against the limit.
        """
        if now is None:
            now = time.time()
        limit = self.get_rate_limit(handler_name)
        if limit is None:
            return None
        conn = self._conn()
        window_start = now - limit["window_seconds"]
        rows = conn.execute(
            "SELECT started_at FROM rate_limit_runs "
            "WHERE handler = ? AND started_at > ? ORDER BY started_at",
            (handler_name, window_start),
        ).fetchall()
        conn.close()
        used = len(rows)
        reset_in = 0.0
        if rows:
            reset_in = max(
                0.0, rows[0]["started_at"] + limit["window_seconds"] - now
            )
        return {
            "handler": handler_name,
            "max_per_window": limit["max_per_window"],
            "window_seconds": limit["window_seconds"],
            "used": used,
            "remaining": max(0, limit["max_per_window"] - used),
            "reset_in": reset_in,
        }

    def _claim_next(self, now, worker_id):
        """Claim the highest priority due job whose handler still has rate budget.

        A job whose handler is at its limit is skipped, not claimed, so other
        handlers keep running and the limited job waits for its window to
        slide. The limit check, the claim, and the recording of the run happen
        in one immediate transaction, so two workers racing for the last unit
        of budget cannot both win it. A handler with no limit configured is
        never checked, so unconfigured handlers run exactly as before.
        """
        conn = self._conn()
        conn.isolation_level = None
        try:
            candidates = conn.execute(
                "SELECT id, handler FROM jobs "
                "WHERE status = 'pending' AND run_at <= ? "
                "ORDER BY priority DESC, run_at ASC, id ASC",
                (now,),
            ).fetchall()
            for candidate in candidates:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    limit = conn.execute(
                        "SELECT * FROM rate_limits WHERE handler = ?",
                        (candidate["handler"],),
                    ).fetchone()
                    if limit is not None:
                        window_start = now - limit["window_seconds"]
                        conn.execute(
                            "DELETE FROM rate_limit_runs "
                            "WHERE handler = ? AND started_at <= ?",
                            (candidate["handler"], window_start),
                        )
                        used = conn.execute(
                            "SELECT COUNT(*) AS n FROM rate_limit_runs "
                            "WHERE handler = ? AND started_at > ?",
                            (candidate["handler"], window_start),
                        ).fetchone()["n"]
                        if used >= limit["max_per_window"]:
                            conn.execute("COMMIT")
                            continue
                    row = conn.execute(
                        "UPDATE jobs SET status = 'running', claimed_by = ? "
                        "WHERE id = ? AND status = 'pending' RETURNING *",
                        (worker_id, candidate["id"]),
                    ).fetchone()
                    if row is None:
                        conn.execute("ROLLBACK")
                        continue
                    if limit is not None:
                        conn.execute(
                            "INSERT INTO rate_limit_runs (handler, started_at) "
                            "VALUES (?, ?)",
                            (candidate["handler"], now),
                        )
                    conn.execute("COMMIT")
                    return dict(row)
                except Exception:
                    if conn.in_transaction:
                        conn.execute("ROLLBACK")
                    raise
            return None
        finally:
            conn.close()

    def process_next(self, now=None, worker_id=None):
        """Claim one due pending job, run it, retry or dead letter it. Returns the job or None.

        A job is due when its run_at time has passed. Future jobs stay pending
        and are skipped, so a delayed job never runs early. Among due jobs,
        higher priority wins first, then earlier run_at, then lower id, so
        priority never lets a future job jump ahead of a due job. A job whose
        handler is out of rate budget is skipped, so a limited handler never
        blocks other handlers, and the skipped job runs once its window slides.

        The claim is atomic, so two workers calling this at the same time can
        never claim the same job. The worker that claimed a job is recorded
        in claimed_by, so you can see who ran what.

        Handlers that accept a second argument get a progress reporter, so
        they can report percent complete and check for cancellation. A job
        cancelled while running ends as cancelled, not completed.
        """
        import json

        if now is None:
            now = time.time()
        worker_id = worker_id or "worker-1"
        job = self._claim_next(now, worker_id)
        if job is None:
            return None
        fn = HANDLERS.get(job["handler"])
        progress = ProgressReporter(self, job["id"])
        try:
            if fn is None:
                raise RuntimeError(f"unknown handler: {job['handler']}")
            if len(inspect.signature(fn).parameters) >= 2:
                result = fn(json.loads(job["payload"]), progress)
            else:
                result = fn(json.loads(job["payload"]))
            conn = self._conn()
            if self.is_cancel_requested(job["id"]):
                conn.execute(
                    "UPDATE jobs SET status = 'cancelled', result = ?, attempts = attempts + 1 WHERE id = ?",
                    (json.dumps(result), job["id"]),
                )
            else:
                conn.execute(
                    "UPDATE jobs SET status = 'completed', result = ?, attempts = attempts + 1 WHERE id = ?",
                    (json.dumps(result), job["id"]),
                )
            conn.commit()
            conn.close()
        except Exception as exc:  # noqa: BLE001, retry then dead letter
            conn = self._conn()
            attempts = job["attempts"] + 1
            if self.is_cancel_requested(job["id"]):
                conn.execute(
                    "UPDATE jobs SET status = 'cancelled', error = ?, attempts = ? WHERE id = ?",
                    (str(exc), attempts, job["id"]),
                )
            elif attempts >= job["max_attempts"]:
                conn.execute(
                    "UPDATE jobs SET status = 'dead', error = ?, attempts = ? WHERE id = ?",
                    (str(exc), attempts, job["id"]),
                )
            else:
                conn.execute(
                    "UPDATE jobs SET status = 'pending', error = ?, attempts = ? WHERE id = ?",
                    (str(exc), attempts, job["id"]),
                )
            conn.commit()
            conn.close()
        return self.get(job["id"])
