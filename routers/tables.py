"""
routers/tables.py
------------------
หน้ารายการตารางทั้งหมด (กรอง/ค้นหา/เรียง) + หน้ารายละเอียดผลตรวจของตารางเดียว
+ ปุ่มตรวจซ้ำเฉพาะตารางนั้น (เป็น scan mode='table')
"""

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from services import store_service as store
from services.scan_manager import scan_manager, ScanAlreadyRunningError

router = APIRouter()
templates = Jinja2Templates(directory="templates")

PAGE_SIZE = 30


@router.get("/tables", response_class=HTMLResponse)
def tables_page(
    request: Request,
    status: str = None,
    database: str = None,
    q: str = None,
    sort: str = "score_asc",
    page: int = 1,
):
    page = max(1, page)
    total = store.count_latest_results(status=status, database=database, search=q)
    rows = store.get_latest_results(
        status=status, database=database, search=q, sort=sort,
        limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE,
    )
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    return templates.TemplateResponse(
        "tables.html",
        {
            "request": request, "active": "tables",
            "rows": rows, "total": total, "page": page, "total_pages": total_pages,
            "status": status or "", "database": database or "", "q": q or "", "sort": sort,
            "databases": store.list_distinct_databases(),
            "running_run_id": scan_manager.active_run_id(),
        },
    )


@router.get("/tables/{database}/{table}", response_class=HTMLResponse)
def table_detail_page(request: Request, database: str, table: str):
    detail = store.get_table_detail(database, table)
    history = store.get_table_history(database, table, limit=30)
    return templates.TemplateResponse(
        "table_detail.html",
        {
            "request": request, "active": "tables",
            "database": database, "table": table,
            "detail": detail, "history": history,
            "running_run_id": scan_manager.active_run_id(),
        },
    )


@router.post("/api/tables/{database}/{table}/recheck")
def recheck_table(database: str, table: str):
    try:
        run_id = scan_manager.start_scan(mode="table", database=database, table=table)
    except ScanAlreadyRunningError as e:
        raise HTTPException(status_code=409, detail=f"มีการสแกนกำลังทำงานอยู่แล้ว (run_id={e})")
    return JSONResponse(status_code=202, content={"run_id": run_id})
