"""Day 1, SQLite backed job queue with retry, dead letter, idempotency."""
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
            "created_at REAL NOT NULL)"
        )
        conn.commit()
        conn.close()

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def enqueue(self, handler_name, payload, max_attempts=3, idempotency_key=None):
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
        cur = conn.execute(
            "INSERT INTO jobs (handler, payload, max_attempts, idempotency_key, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (handler_name, json.dumps(payload), max_attempts, idempotency_key, time.time()),
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

    def process_next(self):
        """Claim one pending job, run it, retry or dead letter it. Returns the job or None."""
        import json

        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM jobs WHERE status = 'pending' ORDER BY id ASC LIMIT 1"
        ).fetchone()
        if row is None:
            conn.close()
            return None
        job = dict(row)
        conn.execute("UPDATE jobs SET status = 'running' WHERE id = ?", (job["id"],))
        conn.commit()
        conn.close()
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
