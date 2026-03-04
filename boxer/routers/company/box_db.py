import pymysql

from boxer.company import settings as cs
from boxer.core import settings as s
from boxer.core.utils import _display_value
from boxer.routers.common.db import _create_db_connection


def _query_recordings_count_by_barcode(barcode: str) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS recordingCount FROM recordings WHERE fullBarcode = %s",
                (barcode,),
            )
            row = cursor.fetchone() or {}
    finally:
        connection.close()

    count = int(row.get("recordingCount") or 0)
    return (
        "*바코드 영상 개수 조회 결과*\n"
        f"• 바코드: `{barcode}`\n"
        f"• recordings row 수: *{count}개*"
    )


def _lookup_device_names_by_barcode(barcode: str) -> list[str]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    sql_candidates = [
        (
            "SELECT DISTINCT d.deviceName AS deviceName "
            "FROM recordings r "
            "JOIN devices d ON d.seq = r.deviceSeq AND d.hospitalSeq = r.hospitalSeq "
            "WHERE r.fullBarcode = %s AND COALESCE(d.deviceName, '') <> '' "
            "LIMIT %s"
        ),
        (
            "SELECT DISTINCT d.deviceName AS deviceName "
            "FROM recordings r "
            "JOIN devices d ON d.seq = r.deviceSeq "
            "WHERE r.fullBarcode = %s AND COALESCE(d.deviceName, '') <> '' "
            "LIMIT %s"
        ),
    ]

    limit = max(1, min(50, cs.LOG_ANALYSIS_MAX_DEVICES * 2))
    last_error: Exception | None = None
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            for sql in sql_candidates:
                try:
                    cursor.execute(sql, (barcode, limit))
                    rows = cursor.fetchall()
                except pymysql.MySQLError as exc:
                    last_error = exc
                    continue

                names: list[str] = []
                seen: set[str] = set()
                for row in rows:
                    name = _display_value(row.get("deviceName"), default="")
                    if not name:
                        continue
                    if name in seen:
                        continue
                    seen.add(name)
                    names.append(name)
                if names:
                    return names
    finally:
        connection.close()

    if last_error:
        raise last_error
    return []
