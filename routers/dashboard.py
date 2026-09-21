"""
routers/dashboard.py
---------------------
หน้าแรก (สรุปคุณภาพภาพรวม) + API สั่งสแกน/ติดตามสถานะสแกนแบบ background job
"""

from typing import List

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

import config
from services import impala_service as svc
from services import store_service as store
from services.scan_manager import scan_manager, ScanAlreadyRunningError
from routers._util import run_public

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/", response_class=HTMLResponse)
def dashboard_page(request: Request):
    summary = store.get_dashboard_summary()
    recent_runs = [run_public(r, log_tail=0) for r in store.list_runs(limit=5)]
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "active": "home",
            "summary": summary,
            "recent_runs": recent_runs,
            "running_run_id": scan_manager.active_run_id(),
            "impala_host": config.IMPALA_HOST or "—",
            "impala_port": config.IMPALA_PORT,
        },
    )


@router.get("/api/impala/ping")
def impala_ping():
    ok, detail = svc.ping()
    return {"ok": ok, "detail": detail}


@router.get("/api/impala/databases")
def impala_databases():
    try:
        conn = svc.get_connection()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    try:
        return {"databases": svc.list_databases(conn)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        conn.close()


@router.get("/api/impala/tables/{database}")
def impala_tables(database: str):
    try:
        conn = svc.get_connection()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    try:
        return {"database": database, "tables": svc.list_tables(conn, database)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        conn.close()


@router.post("/api/scan/start")
def start_scan(mode: str = "full", database: str = None, table: str = None):
    if mode not in ("full", "database", "table"):
        raise HTTPException(status_code=400, detail="mode ต้องเป็น full | database | table")
    if mode in ("database", "table") and not database:
        raise HTTPException(status_code=400, detail="ต้องระบุ database")
    if mode == "table" and not table:
        raise HTTPException(status_code=400, detail="ต้องระบุ table")

    try:
        run_id = scan_manager.start_scan(mode=mode, database=database, table=table)
    except ScanAlreadyRunningError as e:
        raise HTTPException(status_code=409, detail=f"มีการสแกนกำลังทำงานอยู่แล้ว (run_id={e})")

    return JSONResponse(status_code=202, content={"run_id": run_id})


class TargetPair(BaseModel):
    database: str
    table: str


class SelectionRequest(BaseModel):
    targets: List[TargetPair]


@router.post("/api/scan/start/selection")
def start_selection_scan(payload: SelectionRequest):
    if not payload.targets:
        raise HTTPException(status_code=400, detail="ต้องเลือกอย่างน้อย 1 ตาราง")

    targets = [{"database": t.database, "table": t.table} for t in payload.targets]
    try:
        run_id = scan_manager.start_scan(mode="selection", targets=targets)
    except ScanAlreadyRunningError as e:
        raise HTTPException(status_code=409, detail=f"มีการสแกนกำลังทำงานอยู่แล้ว (run_id={e})")

    return JSONResponse(status_code=202, content={"run_id": run_id})


@router.get("/api/scan/running")
def scan_running():
    return {"run_id": scan_manager.active_run_id()}


@router.get("/api/scan/{run_id}/status")
def scan_status(run_id: str, log_tail: int = 30):
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return run_public(run, log_tail=log_tail)


@router.post("/api/scan/{run_id}/cancel")
def scan_cancel(run_id: str):
    ok = scan_manager.cancel(run_id)
    if not ok:
        raise HTTPException(status_code=409, detail="run ไม่ได้กำลังทำงานอยู่ (อาจจบไปแล้ว)")
    return {"ok": True}
