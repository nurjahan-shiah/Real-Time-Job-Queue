"""SQLite backed job queue with retry, dead letter, idempotency, delayed jobs, priority, multiple workers."""
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
            "priority INTEGER NOT NULL DEFAULT 0, claimed_by TEXT)"
        )
        columns = [r[1] for r in conn.execute("PRAGMA table_info(jobs)")]
        if "run_at" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN run_at REAL NOT NULL DEFAULT 0")
        if "priority" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
        if "claimed_by" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN claimed_by TEXT")
        conn.commit()
        conn.close()

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

    def process_next(self, now=None, worker_id=None):
        """Claim one due pending job, run it, retry or dead letter it. Returns the job or None.

        A job is due when its run_at time has passed. Future jobs stay pending
        and are skipped, so a delayed job never runs early. Among due jobs,
        higher priority wins first, then earlier run_at, then lower id, so
        priority never lets a future job jump ahead of a due job.

        The claim is one atomic UPDATE, so two workers calling this at the
        same time can never claim the same job. The worker that claimed a job
        is recorded in claimed_by, so you can see who ran what.
        """
        import json

        if now is None:
            now = time.time()
        worker_id = worker_id or "worker-1"
        conn = self._conn()
        row = conn.execute(
            "UPDATE jobs SET status = 'running', claimed_by = ? WHERE id = ("
            "SELECT id FROM jobs WHERE status = 'pending' AND run_at <= ? "
            "ORDER BY priority DESC, run_at ASC, id ASC LIMIT 1) RETURNING *",
            (worker_id, now),
        ).fetchone()
        conn.commit()
        conn.close()
        if row is None:
            return None
        job = dict(row)
        fn = HANDLERS.get(job["handler"])
        try:
            if fn is None:
                raise RuntimeError(f"unknown handler: {job['handler']}")
            result = fn(json.loads(job["payload"]))
            conn = self._conn()
            conn.execute(
                "UPDATE jobs SET status = 'completed', result = ?, attempts = attempts + 1 WHERE id = ?",
                (json.dumps(result), job["id"]),
            )
            conn.commit()
            conn.close()
        except Exception as exc:  # noqa: BLE001, retry then dead letter
            conn = self._conn()
            attempts = job["attempts"] + 1
            if attempts >= job["max_attempts"]:
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
