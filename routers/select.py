"""
routers/select.py
------------------
หน้าเลือก database + ตารางที่ต้องการตรวจเอง (ไม่ต้องสแกนทั้ง cluster)
"""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from services import impala_service as svc
from services.scan_manager import scan_manager

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/select", response_class=HTMLResponse)
def select_page(request: Request):
    databases = []
    error = None
    try:
        conn = svc.get_connection()
        try:
            databases = svc.list_databases(conn)
        finally:
            conn.close()
    except Exception as e:
        error = str(e)

    return templates.TemplateResponse(
        "select.html",
        {
            "request": request, "active": "select",
            "databases": databases, "error": error,
            "running_run_id": scan_manager.active_run_id(),
        },
    )
