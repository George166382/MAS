"""
result_store.py
---------------
SQLite-backed store mapping trace_id -> pipeline status + result.
SQLite requires no extra infrastructure - suitable for thesis demo.
Swap the connection string for PostgreSQL in production.
"""

import json
import os
import sqlite3
import threading
from datetime import datetime
from enum import Enum

DB_PATH = os.getenv("RESULT_DB_PATH", "results.db")


class Status(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


_lock = threading.Lock()


def _conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    with _lock, _conn() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS results (
                trace_id   TEXT PRIMARY KEY,
                status     TEXT NOT NULL DEFAULT 'pending',
                result     TEXT,
                error      TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def create(trace_id: str):
    now = datetime.utcnow().isoformat()
    with _lock, _conn() as connection:
        connection.execute(
            "INSERT OR IGNORE INTO results (trace_id, status, created_at, updated_at) VALUES (?,?,?,?)",
            (trace_id, Status.PENDING.value, now, now),
        )


def update(trace_id: str, status: Status, result: dict = None, error: str = None):
    now = datetime.utcnow().isoformat()
    result_json = json.dumps(result) if result is not None else None
    with _lock, _conn() as connection:
        connection.execute(
            """
            INSERT INTO results (trace_id, status, result, error, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(trace_id) DO UPDATE SET
                status=excluded.status,
                result=excluded.result,
                error=excluded.error,
                updated_at=excluded.updated_at
            """,
            (trace_id, status.value, result_json, error, now, now),
        )


def get(trace_id: str) -> dict | None:
    with _conn() as connection:
        row = connection.execute(
            "SELECT status, result, error, created_at, updated_at FROM results WHERE trace_id=?",
            (trace_id,),
        ).fetchone()
    if not row:
        return None
    status, result_json, error, created_at, updated_at = row
    return {
        "trace_id": trace_id,
        "status": status,
        "result": json.loads(result_json) if result_json else None,
        "error": error,
        "created_at": created_at,
        "updated_at": updated_at,
    }


init_db()