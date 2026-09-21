"""
impala_service.py
------------------
เชื่อมต่อ Impala + ฟังก์ชันสำรวจ metadata ล้วน ๆ (ไม่ scan ข้อมูลจริง):
SHOW DATABASES / SHOW TABLES / DESCRIBE / SHOW TABLE STATS / SHOW COLUMN STATS

ทุกฟังก์ชันรับ connection ที่เปิดไว้แล้วเป็นพารามิเตอร์ (ยกเว้น get_connection/ping)
เพื่อให้ scan_manager คุมอายุ connection เองได้ (1 connection ต่อ worker thread)
"""

from typing import Optional

from impala.dbapi import connect

import config


def get_connection(database: Optional[str] = None):
    auth = config.IMPALA_AUTH_MECHANISM
    if auth == "LDAP":
        auth = "PLAIN"

    kwargs = {
        "host": config.IMPALA_HOST,
        "port": config.IMPALA_PORT,
        "auth_mechanism": auth,
        "timeout": config.QUERY_TIMEOUT_SECONDS,
    }
    if database:
        kwargs["database"] = database
    if config.IMPALA_USER:
        kwargs["user"] = config.IMPALA_USER
    if auth != "NOSASL" and config.IMPALA_PASSWORD:
        kwargs["password"] = config.IMPALA_PASSWORD
    if config.IMPALA_USE_SSL:
        kwargs["use_ssl"] = True

    return connect(**kwargs)


def ping():
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            cur.close()
        finally:
            conn.close()
        return True, None
    except Exception as e:
        return False, str(e)


def list_databases(conn) -> list:
    cur = conn.cursor()
    cur.execute("SHOW DATABASES")
    rows = cur.fetchall()
    cur.close()
    excluded = {d.lower() for d in config.EXCLUDED_DATABASES}
    names = [str(r[0]) for r in rows if r and r[0]]
    return sorted((n for n in names if n.lower() not in excluded), key=str.lower)


def list_tables(conn, database: str) -> list:
    cur = conn.cursor()
    cur.execute(f"SHOW TABLES IN `{database}`")
    rows = cur.fetchall()
    cur.close()
    return sorted((str(r[0]) for r in rows if r and r[0]), key=str.lower)


def describe_table(conn, database: str, table: str) -> list:
    """คืน list ของ {"name","type","comment"} เรียงตามลำดับคอลัมน์จริงในตาราง"""
    cur = conn.cursor()
    cur.execute(f"DESCRIBE `{database}`.`{table}`")
    rows = cur.fetchall()
    cur.close()

    out = []
    for r in rows:
        if not r or r[0] is None:
            continue
        name = str(r[0]).strip()
        if not name or name.startswith("#"):
            continue
        col_type = str(r[1]).strip().upper() if len(r) > 1 and r[1] is not None else ""
        comment = r[2] if len(r) > 2 else None
        out.append({"name": name, "type": col_type, "comment": comment})
    return out


def _find_header_index(headers, *needles):
    lowered = [h.strip().lower() for h in headers]
    for needle in needles:
        for i, h in enumerate(lowered):
            if h == needle:
                return i
    for needle in needles:
        for i, h in enumerate(lowered):
            if needle in h:
                return i
    return None


def show_table_stats(conn, database: str, table: str) -> dict:
    """คืน {"row_count": int|None, "size": str|None} — row_count เป็น None ถ้ายังไม่เคย COMPUTE STATS
    (metadata-only ไม่ scan ข้อมูล)"""
    cur = conn.cursor()
    cur.execute(f"SHOW TABLE STATS `{database}`.`{table}`")
    rows = cur.fetchall()
    headers = [d[0] for d in cur.description] if cur.description else []
    cur.close()

    if not rows:
        return {"row_count": None, "size": None}

    rows_idx = _find_header_index(headers, "#rows", "rows")
    size_idx = _find_header_index(headers, "size")

    total_row = next(
        (r for r in rows if r and str(r[0]).strip().lower() == "total"), None
    )
    if total_row is not None:
        target_rows = [total_row]
    elif len(rows) == 1:
        target_rows = rows
    else:
        target_rows = [r for r in rows if not (r and str(r[0]).strip().lower() == "total")]

    row_count = None
    if rows_idx is not None:
        values = []
        for r in target_rows:
            v = r[rows_idx] if rows_idx < len(r) else None
            if v is None:
                continue
            try:
                values.append(int(v))
            except (TypeError, ValueError):
                continue
        positive = [v for v in values if v >= 0]
        if positive:
            row_count = sum(positive)
        elif values:
            row_count = None  # ทุกค่าเป็น -1 = ยังไม่เคย COMPUTE STATS

    size = None
    if size_idx is not None and target_rows and size_idx < len(target_rows[0]):
        size = target_rows[0][size_idx]

    return {"row_count": row_count, "size": str(size) if size is not None else None}


def show_column_stats(conn, database: str, table: str) -> dict:
    """คืน {column_name: {"distinct_count": int|None, "num_nulls": int|None}} (metadata-only)"""
    cur = conn.cursor()
    cur.execute(f"SHOW COLUMN STATS `{database}`.`{table}`")
    rows = cur.fetchall()
    headers = [d[0] for d in cur.description] if cur.description else []
    cur.close()

    distinct_idx = _find_header_index(headers, "#distinct values", "distinct values", "distinct")
    nulls_idx = _find_header_index(headers, "#nulls", "nulls")

    out = {}
    for r in rows:
        if not r or r[0] is None:
            continue
        name = str(r[0]).strip()

        distinct = None
        if distinct_idx is not None and distinct_idx < len(r) and r[distinct_idx] is not None:
            try:
                v = int(r[distinct_idx])
                distinct = v if v >= 0 else None
            except (TypeError, ValueError):
                distinct = None

        nulls = None
        if nulls_idx is not None and nulls_idx < len(r) and r[nulls_idx] is not None:
            try:
                v = int(r[nulls_idx])
                nulls = v if v >= 0 else None
            except (TypeError, ValueError):
                nulls = None

        out[name] = {"distinct_count": distinct, "num_nulls": nulls}
    return out
