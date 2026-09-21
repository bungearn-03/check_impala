"""
main.py
-------
FastAPI app: ตรวจคุณภาพตารางใน Impala ทั้ง cluster แบบอัตโนมัติ (dashboard + background scan job)
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from routers import dashboard, tables, history, select
from services import store_service as store

app = FastAPI(title="check_impala — Data Quality")
app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(dashboard.router)
app.include_router(tables.router)
app.include_router(history.router)
app.include_router(select.router)


@app.on_event("startup")
def _startup():
    store.init_db()
