"""
scan_manager.py
----------------
รัน "การสแกน" (ทั้ง cluster / ทั้ง database / ตารางเดียว) เป็น background job
อนุญาตให้รันได้ทีละ 1 การสแกนเท่านั้น (global lock) — ใช้ N worker thread คู่ขนาน
(config.SCAN_CONCURRENCY) แต่ละ worker เปิด Impala connection ของตัวเอง แบ่งตารางกันตรวจ
แบบ round-robin เพื่อกระจายโหลด ไม่ยิง query พร้อมกันหลายพันตารางจนกระทบ cluster
"""

import threading
from datetime import datetime
from uuid import uuid4

import config
from services import impala_service as svc
from services import quality_checks as checks
from services import store_service as store


class ScanAlreadyRunningError(Exception):
    pass


_STATUS_ICON = {"ok": "✅", "warn": "⚠️", "fail": "❌", "error": "🛑"}


class ScanManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._active_run_id = None

    def is_running(self) -> bool:
        with self._lock:
            return self._active_run_id is not None

    def active_run_id(self):
        with self._lock:
            return self._active_run_id

    def start_scan(
        self, mode: str = "full", database: str = None, table: str = None, targets: list = None,
    ) -> str:
        with self._lock:
            if self._active_run_id is not None:
                raise ScanAlreadyRunningError(self._active_run_id)
            run_id = uuid4().hex
            self._active_run_id = run_id

        if mode == "selection":
            db_count = len({t["database"] for t in (targets or [])})
            target = f"เลือกเอง {len(targets or [])} ตาราง ({db_count} database)"
        else:
            target = f"{database}.{table}" if (database and table) else database
        store.create_run(run_id, mode, target=target)

        thread = threading.Thread(target=self._run, args=(run_id, mode, database, table, targets), daemon=True)
        thread.start()
        return run_id

    def cancel(self, run_id: str) -> bool:
        return store.request_cancel(run_id)

    # ── internal ─────────────────────────────────────────────────────────

    def _discover(self, run_id: str, mode: str, database: str, table: str, targets: list = None):
        if mode == "table":
            return [(database, table)]

        if mode == "selection":
            return [(t["database"], t["table"]) for t in (targets or [])]

        conn = svc.get_connection()
        try:
            if mode == "database":
                return [(database, t) for t in svc.list_tables(conn, database)]

            databases = svc.list_databases(conn)
            targets = []
            for db in databases:
                if store.is_cancel_requested(run_id):
                    break
                try:
                    tables = svc.list_tables(conn, db)
                except Exception as e:
                    store.append_run_log(run_id, f"⚠️ ข้าม database `{db}`: {e}")
                    continue
                targets.extend((db, t) for t in tables)
            return targets
        finally:
            conn.close()

    def _worker(self, run_id: str, targets: list, cancelled: threading.Event):
        try:
            conn = svc.get_connection()
        except Exception as e:
            for db, tbl in targets:
                store.append_run_log(run_id, f"🛑 {db}.{tbl}: เชื่อมต่อ Impala ไม่สำเร็จ ({e})")
                result = {
                    "database": db, "table": tbl, "status": "error", "score": None,
                    "row_count": None, "column_count": None,
                    "checked_at": datetime.now().isoformat(), "error": str(e),
                    "checks": {}, "columns": [],
                }
                store.save_table_result(run_id, result)
                store.bump_run_counts(run_id, "error")
            return

        try:
            for db, tbl in targets:
                if cancelled.is_set() or store.is_cancel_requested(run_id):
                    cancelled.set()
                    return
                store.set_run_position(run_id, db, tbl)
                previous = store.get_latest_result(db, tbl)
                try:
                    result = checks.run_table_checks(conn, db, tbl, previous=previous)
                except Exception as e:
                    result = {
                        "database": db, "table": tbl, "status": "error", "score": None,
                        "row_count": None, "column_count": None,
                        "checked_at": datetime.now().isoformat(), "error": str(e),
                        "checks": {}, "columns": [],
                    }
                store.save_table_result(run_id, result)
                store.bump_run_counts(run_id, result["status"])
                icon = _STATUS_ICON.get(result["status"], "•")
                score_txt = result["score"] if result.get("score") is not None else "-"
                store.append_run_log(run_id, f"{icon} {db}.{tbl} — score {score_txt}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _run(self, run_id: str, mode: str, database: str, table: str, targets: list = None):
        try:
            to_check = self._discover(run_id, mode, database, table, targets)
        except Exception as e:
            store.append_run_log(run_id, f"🛑 ค้นหาตาราง/ฐานข้อมูลล้มเหลว: {e}")
            store.finish_run(run_id, "failed")
            with self._lock:
                self._active_run_id = None
            return

        store.set_run_total(run_id, len(to_check))
        store.append_run_log(run_id, f"พบ {len(to_check)} ตาราง เริ่มตรวจ...")

        if not to_check:
            store.append_run_log(run_id, "ไม่พบตารางให้ตรวจ")
            store.finish_run(run_id, "success")
            with self._lock:
                self._active_run_id = None
            return

        n_workers = max(1, min(config.SCAN_CONCURRENCY, len(to_check)))
        chunks = [to_check[i::n_workers] for i in range(n_workers)]
        cancelled = threading.Event()

        threads = [
            threading.Thread(target=self._worker, args=(run_id, chunk, cancelled), daemon=True)
            for chunk in chunks if chunk
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        was_cancelled = cancelled.is_set() or store.is_cancel_requested(run_id)
        final_status = "cancelled" if was_cancelled else "success"
        store.append_run_log(run_id, "⏹ ยกเลิกโดยผู้ใช้" if was_cancelled else "🏁 ตรวจเสร็จสิ้น")
        store.finish_run(run_id, final_status)
        with self._lock:
            self._active_run_id = None


scan_manager = ScanManager()
