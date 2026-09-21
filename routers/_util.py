"""ตัวช่วยเล็ก ๆ ที่ใช้ร่วมกันหลาย router"""

import json

STATUS_LABEL = {
    "ok": "ผ่าน", "warn": "ควรตรวจสอบ", "fail": "มีปัญหา", "error": "ตรวจไม่สำเร็จ",
    "queued": "รอคิว", "running": "กำลังทำงาน", "success": "สำเร็จ",
    "failed": "ล้มเหลว", "cancelled": "ยกเลิกแล้ว",
}


def run_public(run: dict, log_tail: int = 30):
    if run is None:
        return None
    out = dict(run)
    try:
        lines = json.loads(out.pop("log_json", "[]") or "[]")
    except Exception:
        lines = []
    out["log_lines"] = lines[-log_tail:] if log_tail else lines
    out["cancel_requested"] = bool(out.get("cancel_requested"))
    out["status_label"] = STATUS_LABEL.get(out.get("status"), out.get("status"))
    return out
