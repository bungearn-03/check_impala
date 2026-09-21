"""
config.py
---------
จุดเดียวสำหรับอ่านค่า config ทั้งหมดของ check_impala (env vars + threshold ของแต่ละ check)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env", override=True)

# ── Impala connection ────────────────────────────────────────────────────────
IMPALA_HOST = os.getenv("IMPALA_HOST")
IMPALA_PORT = int(os.getenv("IMPALA_PORT", "21050"))
IMPALA_USER = os.getenv("IMPALA_USER")
IMPALA_PASSWORD = os.getenv("IMPALA_PASSWORD")
IMPALA_AUTH_MECHANISM = (
    os.getenv("IMPALA_AUTH_MECHANISM")
    or ("LDAP" if IMPALA_USER and IMPALA_PASSWORD else "NOSASL")
).strip().upper()
IMPALA_USE_SSL = (os.getenv("IMPALA_USE_SSL", "false")).strip().lower() in ("true", "1", "yes", "y")

# ── ฐานข้อมูลที่ไม่ต้องสแกน (ของ Impala เอง ไม่ใช่ข้อมูลธุรกิจ) ──────────────────
EXCLUDED_DATABASES = {"_impala_builtins", "information_schema", "sys"}

# ── เกณฑ์ป้องกันการสแกนตารางใหญ่จนกระทบ cluster ─────────────────────────────
# ตารางที่แถวเยอะกว่านี้: ข้าม live-scan checks (completeness/uniqueness/freshness
# ที่ต้องอ่านข้อมูลจริง) ใช้เฉพาะ SHOW TABLE STATS / SHOW COLUMN STATS (metadata, ไม่ scan)
MAX_ROWS_FOR_LIVE_SCAN = int(os.getenv("MAX_ROWS_FOR_LIVE_SCAN", "2000000"))
# ตารางที่แถวเยอะกว่านี้: ข้ามเฉพาะเช็คแถวซ้ำทั้งแถว (GROUP BY ทุกคอลัมน์ หนักกว่าเช็คอื่น)
MAX_ROWS_FOR_DUP_CHECK = int(os.getenv("MAX_ROWS_FOR_DUP_CHECK", "1000000"))
# ตารางที่คอลัมน์เยอะกว่านี้: ข้าม live-scan completeness (query เดียวมี expression เยอะเกินไป)
MAX_COLUMNS_FOR_LIVE_SCAN = int(os.getenv("MAX_COLUMNS_FOR_LIVE_SCAN", "80"))

# ── เกณฑ์ freshness ──────────────────────────────────────────────────────────
FRESHNESS_STALE_DAYS = int(os.getenv("FRESHNESS_STALE_DAYS", "7"))

# ── เกณฑ์ volume anomaly ─────────────────────────────────────────────────────
ROW_COUNT_DROP_ALERT_PCT = float(os.getenv("ROW_COUNT_DROP_ALERT_PCT", "20"))

# ── คะแนนคุณภาพ (0-100) → สถานะ ──────────────────────────────────────────────
SCORE_WARN_BELOW = float(os.getenv("SCORE_WARN_BELOW", "90"))   # ต่ำกว่านี้ = warn
SCORE_FAIL_BELOW = float(os.getenv("SCORE_FAIL_BELOW", "65"))   # ต่ำกว่านี้ = fail

# ── การสแกน ──────────────────────────────────────────────────────────────────
SCAN_CONCURRENCY = int(os.getenv("SCAN_CONCURRENCY", "3"))       # จำนวน connection ขนานตอนสแกน
QUERY_TIMEOUT_SECONDS = int(os.getenv("QUERY_TIMEOUT_SECONDS", "120"))

# ── SQLite (เก็บผลตรวจ/ประวัติการรัน) ────────────────────────────────────────
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = str(DATA_DIR / "quality.db")
