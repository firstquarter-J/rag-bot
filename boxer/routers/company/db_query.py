import json
from typing import Any

from boxer.core import settings as s


def _extract_db_query(question: str) -> str | None:
    normalized = (question or "").strip()
    lowered = normalized.lower()
    if lowered.startswith("db 조회"):
        return normalized[5:].strip()
    if lowered.startswith("db조회"):
        return normalized[4:].strip()
    return None


def _format_db_query_result(result: dict[str, Any]) -> str:
    rows = result.get("rows") or []
    rowcount = result.get("rowcount")

    if not rows:
        return "DB 조회 결과가 없어"

    payload = json.dumps(rows, ensure_ascii=False, default=str)
    if len(payload) > s.DB_QUERY_MAX_RESULT_CHARS:
        payload = payload[: s.DB_QUERY_MAX_RESULT_CHARS] + "...(truncated)"
    if isinstance(rowcount, int) and rowcount > len(rows):
        summary = f"DB 조회 결과 {rowcount}건 중 {len(rows)}건만 보여줄게"
    else:
        summary = f"DB 조회 결과 {len(rows)}건"
    return f"{summary}\n```json\n{payload}\n```"
