"""
quality_checks.py
------------------
ตรรกะตรวจคุณภาพของ 1 ตาราง ครอบคลุม:
  - completeness  (% NULL / ค่าว่าง ต่อคอลัมน์)
  - uniqueness    (แถวซ้ำทั้งแถว + คอลัมน์ที่มีค่าไม่ซ้ำ = key candidate)
  - freshness     (คอลัมน์วันที่/เวลาล่าสุด เทียบ SLA)
  - volume        (จำนวนแถวเทียบครั้งก่อน)
  - schema drift  (คอลัมน์/ชนิดข้อมูลเทียบครั้งก่อน)
  - governance    (เคย COMPUTE STATS หรือยัง)

ใช้ SHOW TABLE STATS / SHOW COLUMN STATS (metadata, ไม่ scan) เป็นค่าเริ่มต้นเสมอ
และจะ scan ข้อมูลจริง (query เดียว รวมทุกคอลัมน์) เฉพาะตารางที่ไม่ใหญ่เกิน threshold ใน config
เพื่อไม่ให้การสแกนทั้ง cluster กระทบ query จริงของคนอื่น
"""

from datetime import datetime

import config
from services import impala_service as svc

_STRINGY_PREFIXES = ("STRING", "VARCHAR", "CHAR")
_DATEY_PREFIXES = ("TIMESTAMP", "DATE")
_FRESH_KEYWORDS = ("update", "modif", "refresh", "load", "sync", "etl", "process")


def _now():
    return datetime.now()


def _is_stringy(col_type: str) -> bool:
    return bool(col_type) and col_type.upper().startswith(_STRINGY_PREFIXES)


def _is_datey(col_type: str) -> bool:
    return bool(col_type) and col_type.upper().startswith(_DATEY_PREFIXES)


def _fq(database: str, table: str) -> str:
    return f"`{database}`.`{table}`"


def _scan_columns(conn, database, table, columns):
    """query เดียว ผ่าน conditional aggregation: COUNT(*) + null count + NDV (approx distinct)
    + blank count (เฉพาะคอลัมน์ string) ของทุกคอลัมน์ในตาราง"""
    select_parts = ["COUNT(*) AS row_cnt"]
    for i, col in enumerate(columns):
        cname = f"`{col['name']}`"
        select_parts.append(f"SUM(CASE WHEN {cname} IS NULL THEN 1 ELSE 0 END) AS n{i}")
        select_parts.append(f"NDV({cname}) AS d{i}")
        if _is_stringy(col["type"]):
            select_parts.append(
                f"SUM(CASE WHEN {cname} IS NOT NULL AND length(trim({cname})) = 0 "
                f"THEN 1 ELSE 0 END) AS b{i}"
            )

    sql = f"SELECT {', '.join(select_parts)} FROM {_fq(database, table)}"
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    aliases = [d[0].lower() for d in cur.description]
    cur.close()

    values = dict(zip(aliases, row))
    row_cnt = int(values.get("row_cnt") or 0)

    per_column = {}
    for i, col in enumerate(columns):
        n = values.get(f"n{i}")
        d = values.get(f"d{i}")
        b = values.get(f"b{i}") if _is_stringy(col["type"]) else None
        per_column[col["name"]] = {
            "null_count": int(n) if n is not None else None,
            "distinct_count": int(d) if d is not None else None,
            "blank_count": int(b) if b is not None else None,
        }
    return row_cnt, per_column


def _completeness_and_uniqueness_source(columns, row_count, col_stats):
    """เมื่อไม่ live-scan: ใช้ SHOW COLUMN STATS แทน (อาจไม่มีถ้ายังไม่เคย COMPUTE STATS)"""
    per_column = {}
    for col in columns:
        cs = col_stats.get(col["name"], {})
        per_column[col["name"]] = {
            "null_count": cs.get("num_nulls"),
            "distinct_count": cs.get("distinct_count"),
            "blank_count": None,
        }
    return per_column


def _pick_freshness_column(datey_columns):
    def sort_key(c):
        name_l = c["name"].lower()
        prioritized = 0 if any(k in name_l for k in _FRESH_KEYWORDS) else 1
        return (prioritized, c["name"])

    return sorted(datey_columns, key=sort_key)[0]


def _check_freshness(conn, database, table, columns, do_live_scan):
    datey_columns = [c for c in columns if _is_datey(c["type"])]
    if not datey_columns:
        return {
            "column": None, "max_value": None, "age_days": None,
            "status": "unknown", "note": "ไม่พบคอลัมน์ชนิดวันที่/เวลาในตาราง",
            "threshold_days": config.FRESHNESS_STALE_DAYS,
        }

    chosen = _pick_freshness_column(datey_columns)

    if not do_live_scan:
        return {
            "column": chosen["name"], "max_value": None, "age_days": None,
            "status": "unknown", "note": "ข้ามเพราะตารางใหญ่เกินเกณฑ์ live-scan",
            "threshold_days": config.FRESHNESS_STALE_DAYS,
        }

    try:
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(`{chosen['name']}`) FROM {_fq(database, table)}")
        value = cur.fetchone()[0]
        cur.close()
    except Exception as e:
        return {
            "column": chosen["name"], "max_value": None, "age_days": None,
            "status": "unknown", "note": f"ตรวจไม่สำเร็จ: {e}",
            "threshold_days": config.FRESHNESS_STALE_DAYS,
        }

    if value is None:
        return {
            "column": chosen["name"], "max_value": None, "age_days": None,
            "status": "unknown", "note": "ไม่มีค่าในคอลัมน์นี้เลย (ทั้งหมดเป็น NULL หรือตารางว่าง)",
            "threshold_days": config.FRESHNESS_STALE_DAYS,
        }

    if isinstance(value, datetime):
        age_days = (_now() - value).days
        status = "stale" if age_days > config.FRESHNESS_STALE_DAYS else "fresh"
        return {
            "column": chosen["name"], "max_value": value.isoformat(), "age_days": age_days,
            "status": status, "note": None, "threshold_days": config.FRESHNESS_STALE_DAYS,
        }

    return {
        "column": chosen["name"], "max_value": str(value), "age_days": None,
        "status": "unknown", "note": "อ่านค่าได้แต่แปลงเป็นวันที่ไม่สำเร็จ",
        "threshold_days": config.FRESHNESS_STALE_DAYS,
    }


def _check_uniqueness(conn, database, table, columns, row_count, do_dup_check, dup_skip_reason):
    key_candidates = []  # เติมทีหลังจาก columns_detail
    result = {
        "duplicate_row_count": None, "duplicate_row_pct": None,
        "checked": False, "skip_reason": dup_skip_reason, "key_candidates": key_candidates,
    }
    if not do_dup_check:
        return result

    col_list = ", ".join(f"`{c['name']}`" for c in columns)
    sql = (
        f"SELECT COALESCE(SUM(cnt - 1), 0) FROM ("
        f"SELECT COUNT(*) AS cnt FROM {_fq(database, table)} "
        f"GROUP BY {col_list} HAVING COUNT(*) > 1) x"
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        dup = cur.fetchone()[0]
        cur.close()
    except Exception as e:
        result["skip_reason"] = f"ตรวจไม่สำเร็จ: {e}"
        return result

    dup = int(dup or 0)
    result["duplicate_row_count"] = dup
    result["checked"] = True
    result["skip_reason"] = None
    if row_count:
        result["duplicate_row_pct"] = round(dup / row_count * 100, 3)
    return result


def _check_volume(row_count, previous):
    prev_row_count = previous.get("row_count") if previous else None
    change_pct = None
    if row_count is None:
        status = "unknown"
    elif prev_row_count is None:
        status = "empty" if row_count == 0 else "new"
    elif row_count == 0 and prev_row_count > 0:
        status = "emptied"
    elif prev_row_count > 0:
        change_pct = round((row_count - prev_row_count) / prev_row_count * 100, 2)
        drop_limit = -abs(config.ROW_COUNT_DROP_ALERT_PCT)
        status = "dropped" if change_pct <= drop_limit else "ok"
    else:
        status = "ok"
    return {
        "row_count": row_count, "previous_row_count": prev_row_count,
        "change_pct": change_pct, "status": status,
    }


def _check_schema_drift(columns, previous):
    current_map = {c["name"]: c["type"] for c in columns}
    prev_columns = previous.get("schema_columns") if previous else None

    if prev_columns is None:
        return {
            "status": "new", "added": [], "removed": [], "type_changed": [],
            "schema_columns": [{"name": n, "type": t} for n, t in current_map.items()],
        }

    prev_map = {c["name"]: c["type"] for c in prev_columns}
    added = sorted(set(current_map) - set(prev_map))
    removed = sorted(set(prev_map) - set(current_map))
    type_changed = [
        {"column": k, "from": prev_map[k], "to": current_map[k]}
        for k in sorted(current_map)
        if k in prev_map and current_map[k] != prev_map[k]
    ]
    status = "changed" if (added or removed or type_changed) else "unchanged"
    return {
        "status": status, "added": added, "removed": removed, "type_changed": type_changed,
        "schema_columns": [{"name": n, "type": t} for n, t in current_map.items()],
    }


def compute_score(checks: dict) -> float:
    score = 100.0

    comp = checks["completeness"]
    if comp.get("avg_missing_pct") is not None:
        score -= min(30.0, comp["avg_missing_pct"] * 0.3)

    uniq = checks["uniqueness"]
    if uniq.get("checked"):
        score -= min(30.0, (uniq.get("duplicate_row_pct") or 0) * 0.3)

    fresh = checks["freshness"]
    if fresh.get("status") == "stale":
        score -= 15.0

    vol = checks["volume"]
    if vol.get("status") == "empty":
        score -= 20.0
    elif vol.get("status") == "emptied":
        score -= 25.0
    elif vol.get("status") == "dropped":
        score -= 15.0

    if not checks["governance"].get("stats_computed"):
        score -= 5.0

    return round(max(0.0, min(100.0, score)), 1)


def status_from_score(score) -> str:
    if score is None:
        return "error"
    if score < config.SCORE_FAIL_BELOW:
        return "fail"
    if score < config.SCORE_WARN_BELOW:
        return "warn"
    return "ok"


def run_table_checks(conn, database: str, table: str, previous: dict = None) -> dict:
    checked_at = _now().isoformat()

    try:
        columns = svc.describe_table(conn, database, table)
    except Exception as e:
        return {
            "database": database, "table": table, "status": "error", "score": None,
            "row_count": None, "column_count": None, "checked_at": checked_at,
            "error": f"DESCRIBE ล้มเหลว: {e}", "checks": {}, "columns": [],
        }

    stats_error = None
    try:
        table_stats = svc.show_table_stats(conn, database, table)
    except Exception as e:
        table_stats = {"row_count": None, "size": None}
        stats_error = str(e)

    try:
        col_stats = svc.show_column_stats(conn, database, table)
    except Exception:
        col_stats = {}

    row_count = table_stats.get("row_count")
    column_count = len(columns)

    # ถ้ายังไม่เคย COMPUTE STATS, SHOW TABLE STATS จะได้ #Rows = -1 (row_count เป็น None)
    # ทั้งที่ตารางมีข้อมูลจริง — นับจริงด้วย COUNT(*) แทน เพื่อให้ค่าที่แสดงเป็นข้อมูลจริงเสมอ
    count_error = None
    if row_count is None:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {_fq(database, table)}")
            cnt = cur.fetchone()[0]
            cur.close()
            row_count = int(cnt) if cnt is not None else None
        except Exception as e:
            count_error = f"COUNT(*) ล้มเหลว: {e}"

    do_live_scan = (
        column_count > 0
        and column_count <= config.MAX_COLUMNS_FOR_LIVE_SCAN
        and row_count is not None
        and row_count <= config.MAX_ROWS_FOR_LIVE_SCAN
    )
    scan_error = None
    per_column_raw = {}

    if do_live_scan:
        try:
            live_row_count, per_column_raw = _scan_columns(conn, database, table, columns)
            row_count = live_row_count  # ใช้ตัวเลขที่ scan จริง (ล่าสุดกว่าค่าจาก stats เสมอ)
        except Exception as e:
            scan_error = str(e)
            do_live_scan = False
            per_column_raw = _completeness_and_uniqueness_source(columns, row_count, col_stats)
    else:
        per_column_raw = _completeness_and_uniqueness_source(columns, row_count, col_stats)

    dup_skip_reason = None
    if not do_live_scan:
        dup_skip_reason = "ข้ามเพราะตารางใหญ่เกินเกณฑ์ live-scan" if row_count is not None else (
            "ข้ามเพราะนับจำนวนแถวไม่สำเร็จ"
        )
    do_dup_check = do_live_scan and row_count is not None and row_count <= config.MAX_ROWS_FOR_DUP_CHECK
    if do_live_scan and not do_dup_check:
        dup_skip_reason = f"ข้ามเพราะแถวเกิน {config.MAX_ROWS_FOR_DUP_CHECK:,} แถว"

    columns_detail = []
    missing_pcts = []
    for col in columns:
        raw = per_column_raw.get(col["name"], {})
        null_count = raw.get("null_count")
        blank_count = raw.get("blank_count")
        distinct_count = raw.get("distinct_count")

        missing_count = None
        if null_count is not None:
            missing_count = null_count + (blank_count or 0)
        missing_pct = None
        if missing_count is not None and row_count:
            missing_pct = round(missing_count / row_count * 100, 3)
            missing_pcts.append(missing_pct)

        is_constant = bool(distinct_count is not None and row_count and row_count > 0 and distinct_count <= 1)
        is_key_candidate = bool(
            distinct_count is not None and row_count and row_count > 0 and distinct_count == row_count
        )

        columns_detail.append({
            "name": col["name"], "type": col["type"],
            "null_count": null_count, "blank_count": blank_count,
            "missing_count": missing_count, "missing_pct": missing_pct,
            "distinct_count": distinct_count,
            "is_constant": is_constant, "is_key_candidate": is_key_candidate,
            "source": "scan" if do_live_scan else ("stats" if null_count is not None else "unavailable"),
        })

    avg_missing_pct = round(sum(missing_pcts) / len(missing_pcts), 3) if missing_pcts else None
    worst_columns = sorted(
        (c for c in columns_detail if c["missing_pct"]),
        key=lambda c: c["missing_pct"], reverse=True,
    )[:5]

    completeness = {
        "avg_missing_pct": avg_missing_pct,
        "worst_columns": [
            {"column": c["name"], "missing_pct": c["missing_pct"], "missing_count": c["missing_count"]}
            for c in worst_columns
        ],
        "source": "scan" if do_live_scan else "stats",
        "checked_column_count": sum(1 for c in columns_detail if c["missing_pct"] is not None),
    }

    uniqueness = _check_uniqueness(conn, database, table, columns, row_count, do_dup_check, dup_skip_reason)
    uniqueness["key_candidates"] = [c["name"] for c in columns_detail if c["is_key_candidate"]]

    freshness = _check_freshness(conn, database, table, columns, do_live_scan)
    volume = _check_volume(row_count, previous)
    schema = _check_schema_drift(columns, previous)
    governance = {
        "stats_computed": table_stats.get("row_count") is not None,
        "note": stats_error or (None if table_stats.get("row_count") is not None else "ยังไม่เคยรัน COMPUTE STATS"),
    }

    checks = {
        "completeness": completeness,
        "uniqueness": uniqueness,
        "freshness": freshness,
        "volume": volume,
        "schema": schema,
        "governance": governance,
    }

    score = compute_score(checks)
    status = status_from_score(score)

    note_parts = [n for n in [count_error, scan_error] if n]
    return {
        "database": database, "table": table, "status": status, "score": score,
        "row_count": row_count, "column_count": column_count,
        "size": table_stats.get("size"), "checked_at": checked_at,
        "live_scanned": do_live_scan, "error": "; ".join(note_parts) or None,
        "checks": checks, "columns": columns_detail,
        "schema_columns": schema["schema_columns"],
    }
