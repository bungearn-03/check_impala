"""
store_service.py
-----------------
เก็บผลตรวจคุณภาพ + ประวัติการรันไว้ใน SQLite ตัวเดียว (data/quality.db)
  - runs            : 1 แถวต่อการสั่งสแกน 1 ครั้ง (ทั้ง cluster / ทั้ง database / ตารางเดียว)
  - table_results   : ผลตรวจทุกครั้งของทุกตาราง (ใช้ทำกราฟ/ประวัติย้อนหลัง)
  - latest_results  : ผลล่าสุดของแต่ละตาราง (1 แถวต่อตาราง — ใช้เป็น "previous" ตอนตรวจครั้งถัดไป
                       เพื่อ diff volume/schema drift, และใช้เป็นแหล่งข้อมูลของหน้า dashboard/ตารางทั้งหมด)

ใช้ lock เดียวคุมทั้งไฟล์: ปริมาณงานเขียนของแอปนี้ไม่สูงพอที่จะต้องทำ connection pool,
กันปัญหา "database is locked" จากหลาย worker thread เขียนพร้อมกันได้ตรงไปตรงมาที่สุด
"""

import json
import sqlite3
import threading
from datetime import datetime, timezone

import config

_lock = threading.Lock()

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    target TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    total_tables INTEGER DEFAULT 0,
    done_tables INTEGER DEFAULT 0,
    ok_count INTEGER DEFAULT 0,
    warn_count INTEGER DEFAULT 0,
    fail_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    current_database TEXT,
    current_table TEXT,
    started_at TEXT,
    finished_at TEXT,
    cancel_requested INTEGER DEFAULT 0,
    log_json TEXT DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS table_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,
    score REAL,
    row_count INTEGER,
    column_count INTEGER,
    error TEXT,
    checked_at TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tr_run ON table_results(run_id);
CREATE INDEX IF NOT EXISTS idx_tr_db_table ON table_results(database_name, table_name);

CREATE TABLE IF NOT EXISTS latest_results (
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,
    score REAL,
    row_count INTEGER,
    column_count INTEGER,
    error TEXT,
    checked_at TEXT NOT NULL,
    run_id TEXT,
    detail_json TEXT NOT NULL,
    PRIMARY KEY (database_name, table_name)
);
CREATE INDEX IF NOT EXISTS idx_lr_status ON latest_results(status);
"""


def _connect():
    conn = sqlite3.connect(config.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _now():
    return datetime.now(timezone.utc).isoformat()


def init_db():
    with _lock:
        conn = _connect()
        try:
            conn.executescript(_SCHEMA_SQL)
            conn.commit()
        finally:
            conn.close()


# ── runs ──────────────────────────────────────────────────────────────────

def create_run(run_id: str, mode: str, target: str = None):
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                "INSERT INTO runs (id, mode, target, status, started_at) VALUES (?, ?, ?, 'running', ?)",
                (run_id, mode, target, _now()),
            )
            conn.commit()
        finally:
            conn.close()


def set_run_total(run_id: str, total_tables: int):
    with _lock:
        conn = _connect()
        try:
            conn.execute("UPDATE runs SET total_tables = ? WHERE id = ?", (total_tables, run_id))
            conn.commit()
        finally:
            conn.close()


def set_run_position(run_id: str, database: str, table: str):
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                "UPDATE runs SET current_database = ?, current_table = ? WHERE id = ?",
                (database, table, run_id),
            )
            conn.commit()
        finally:
            conn.close()


def bump_run_counts(run_id: str, status: str):
    field = {"ok": "ok_count", "warn": "warn_count", "fail": "fail_count", "error": "error_count"}.get(
        status, "error_count"
    )
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                f"UPDATE runs SET done_tables = done_tables + 1, {field} = {field} + 1 WHERE id = ?",
                (run_id,),
            )
            conn.commit()
        finally:
            conn.close()


def append_run_log(run_id: str, line: str, keep_last: int = 200):
    with _lock:
        conn = _connect()
        try:
            row = conn.execute("SELECT log_json FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                return
            lines = json.loads(row["log_json"] or "[]")
            lines.append(line)
            lines = lines[-keep_last:]
            conn.execute("UPDATE runs SET log_json = ? WHERE id = ?", (json.dumps(lines), run_id))
            conn.commit()
        finally:
            conn.close()


def request_cancel(run_id: str) -> bool:
    with _lock:
        conn = _connect()
        try:
            row = conn.execute("SELECT status FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None or row["status"] != "running":
                return False
            conn.execute("UPDATE runs SET cancel_requested = 1 WHERE id = ?", (run_id,))
            conn.commit()
            return True
        finally:
            conn.close()


def is_cancel_requested(run_id: str) -> bool:
    with _lock:
        conn = _connect()
        try:
            row = conn.execute("SELECT cancel_requested FROM runs WHERE id = ?", (run_id,)).fetchone()
            return bool(row and row["cancel_requested"])
        finally:
            conn.close()


def finish_run(run_id: str, status: str):
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                "UPDATE runs SET status = ?, finished_at = ?, current_database = NULL, current_table = NULL "
                "WHERE id = ?",
                (status, _now(), run_id),
            )
            conn.commit()
        finally:
            conn.close()


def get_run(run_id: str):
    with _lock:
        conn = _connect()
        try:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


def list_runs(limit: int = 20):
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def get_running_run():
    with _lock:
        conn = _connect()
        try:
            row = conn.execute("SELECT * FROM runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1").fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


# ── table results ────────────────────────────────────────────────────────

def save_table_result(run_id: str, result: dict):
    detail_json = json.dumps(result, ensure_ascii=False, default=str)
    args = (
        run_id, result["database"], result["table"], result["status"], result.get("score"),
        result.get("row_count"), result.get("column_count"), result.get("error"),
        result["checked_at"], detail_json,
    )
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                "INSERT INTO table_results "
                "(run_id, database_name, table_name, status, score, row_count, column_count, error, "
                " checked_at, detail_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                args,
            )
            conn.execute(
                "INSERT INTO latest_results "
                "(database_name, table_name, status, score, row_count, column_count, error, checked_at, "
                " run_id, detail_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(database_name, table_name) DO UPDATE SET "
                "status=excluded.status, score=excluded.score, row_count=excluded.row_count, "
                "column_count=excluded.column_count, error=excluded.error, checked_at=excluded.checked_at, "
                "run_id=excluded.run_id, detail_json=excluded.detail_json",
                (
                    result["database"], result["table"], result["status"], result.get("score"),
                    result.get("row_count"), result.get("column_count"), result.get("error"),
                    result["checked_at"], run_id, detail_json,
                ),
            )
            conn.commit()
        finally:
            conn.close()


def get_latest_result(database: str, table: str):
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT detail_json FROM latest_results WHERE database_name = ? AND table_name = ?",
                (database, table),
            ).fetchone()
            return json.loads(row["detail_json"]) if row else None
        finally:
            conn.close()


def get_latest_results(status: str = None, database: str = None, search: str = None,
                        sort: str = "score_asc", limit: int = None, offset: int = 0):
    where = []
    args = []
    if status:
        where.append("status = ?")
        args.append(status)
    if database:
        where.append("database_name = ?")
        args.append(database)
    if search:
        where.append("(database_name LIKE ? OR table_name LIKE ?)")
        like = f"%{search}%"
        args += [like, like]
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    order_map = {
        "score_asc": "score ASC NULLS LAST" if sqlite3.sqlite_version_info >= (3, 30, 0) else "score IS NULL, score ASC",
        "score_desc": "score DESC NULLS LAST" if sqlite3.sqlite_version_info >= (3, 30, 0) else "score IS NULL, score DESC",
        "name": "database_name ASC, table_name ASC",
        "checked_at_desc": "checked_at DESC",
        "row_count_desc": "row_count DESC",
    }
    order_sql = order_map.get(sort, order_map["score_asc"])

    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ? OFFSET ?"
        args += [limit, offset]

    sql = (
        "SELECT database_name, table_name, status, score, row_count, column_count, error, checked_at "
        f"FROM latest_results {where_sql} ORDER BY {order_sql} {limit_sql}"
    )
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(sql, args).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def count_latest_results(status: str = None, database: str = None, search: str = None) -> int:
    where = []
    args = []
    if status:
        where.append("status = ?")
        args.append(status)
    if database:
        where.append("database_name = ?")
        args.append(database)
    if search:
        where.append("(database_name LIKE ? OR table_name LIKE ?)")
        like = f"%{search}%"
        args += [like, like]
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(f"SELECT COUNT(*) AS c FROM latest_results {where_sql}", args).fetchone()
            return row["c"]
        finally:
            conn.close()


def get_table_detail(database: str, table: str):
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT detail_json FROM latest_results WHERE database_name = ? AND table_name = ?",
                (database, table),
            ).fetchone()
            return json.loads(row["detail_json"]) if row else None
        finally:
            conn.close()


def get_table_history(database: str, table: str, limit: int = 30):
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                "SELECT checked_at, score, status, row_count FROM table_results "
                "WHERE database_name = ? AND table_name = ? ORDER BY id DESC LIMIT ?",
                (database, table, limit),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]
        finally:
            conn.close()


def get_run_results(run_id: str, status: str = None):
    where = "WHERE run_id = ?"
    args = [run_id]
    if status:
        where += " AND status = ?"
        args.append(status)
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                "SELECT database_name, table_name, status, score, row_count, column_count, error, checked_at "
                f"FROM table_results {where} ORDER BY database_name, table_name",
                args,
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def get_dashboard_summary():
    with _lock:
        conn = _connect()
        try:
            total = conn.execute("SELECT COUNT(*) AS c FROM latest_results").fetchone()["c"]
            by_status = {
                r["status"]: r["c"]
                for r in conn.execute(
                    "SELECT status, COUNT(*) AS c FROM latest_results GROUP BY status"
                ).fetchall()
            }
            avg_score_row = conn.execute(
                "SELECT AVG(score) AS a FROM latest_results WHERE score IS NOT NULL"
            ).fetchone()
            db_count = conn.execute(
                "SELECT COUNT(DISTINCT database_name) AS c FROM latest_results"
            ).fetchone()["c"]
            stale_stats = conn.execute(
                "SELECT COUNT(*) AS c FROM latest_results "
                "WHERE detail_json LIKE '%\"stats_computed\": false%'"
            ).fetchone()["c"]
            return {
                "total_tables": total,
                "ok_count": by_status.get("ok", 0),
                "warn_count": by_status.get("warn", 0),
                "fail_count": by_status.get("fail", 0),
                "error_count": by_status.get("error", 0),
                "avg_score": round(avg_score_row["a"], 1) if avg_score_row["a"] is not None else None,
                "database_count": db_count,
                "no_stats_count": stale_stats,
            }
        finally:
            conn.close()


def list_distinct_databases():
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                "SELECT DISTINCT database_name FROM latest_results ORDER BY database_name"
            ).fetchall()
            return [r["database_name"] for r in rows]
        finally:
            conn.close()
