"""
routers/history.py
--------------------
ประวัติการสแกนทั้งหมด + รายละเอียดผลตรวจของการสแกนแต่ละครั้ง
"""

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from services import store_service as store
from services.scan_manager import scan_manager
from routers._util import run_public

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/history", response_class=HTMLResponse)
def history_page(request: Request):
    runs = [run_public(r, log_tail=0) for r in store.list_runs(limit=50)]
    return templates.TemplateResponse(
        "history.html",
        {"request": request, "active": "history", "runs": runs},
    )


@router.get("/history/{run_id}", response_class=HTMLResponse)
def run_detail_page(request: Request, run_id: str, status: str = None):
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    results = store.get_run_results(run_id, status=status)
    return templates.TemplateResponse(
        "run_detail.html",
        {
            "request": request, "active": "history",
            "run": run_public(run, log_tail=200), "results": results, "status": status or "",
            "running_run_id": scan_manager.active_run_id(),
        },
    )
