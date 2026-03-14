#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
VERIFY_DB_PATH = PROJECT_ROOT / "data" / "usage_help_verify.db"
VERIFY_USER_ID = "U_USAGE_VERIFY"
PUBLIC_EXAMPLE_BARCODE = "12345678910"
VERIFY_LIVE_BARCODE = os.getenv("VERIFY_LIVE_BARCODE", "43032748143").strip() or "43032748143"
sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("REQUEST_LOG_SQLITE_ENABLED", "true")
os.environ.setdefault("REQUEST_LOG_SQLITE_PATH", str(VERIFY_DB_PATH))
os.environ.setdefault("REQUEST_LOG_SQLITE_INIT_ON_STARTUP", "true")
os.environ.setdefault("REQUEST_LOG_SQLITE_S3_RESTORE_ON_STARTUP", "false")
os.environ.setdefault("REQUEST_LOG_SQLITE_S3_BACKUP_ENABLED", "false")
os.environ.setdefault("REQUEST_LOG_QUERY_ALLOWED_USER_IDS", VERIFY_USER_ID)
os.environ.setdefault("APP_USER_LOOKUP_ALLOWED_USER_IDS", VERIFY_USER_ID)

from slack_sdk.web.client import WebClient

WebClient.auth_test = lambda self, **kwargs: {  # type: ignore[method-assign]
    "ok": True,
    "user_id": "U_SAMPLE",
    "team_id": "T_SAMPLE",
    "bot_id": "B_SAMPLE",
}

import boxer.adapters.common.slack as common_slack
import boxer.adapters.company.slack as company_slack
import boxer.routers.company.device_file_probe as device_file_probe
from boxer.company import settings as cs
from boxer.core import settings as s
from boxer.routers.common.request_log import (
    _initialize_request_log_storage,
    _save_request_log_record,
)
from boxer.routers.company.usage_help import _build_usage_help_response

_BULLET_CODE_LINE_PATTERN = re.compile(r"^• `(.+)`$")
_FAIL_TEXT_TOKENS = (
    "오류가 발생했어",
    "기능이 꺼져 있어",
    "설정이 부족해",
    "질문 내용을 같이 보내줘",
    "지원 기능이 궁금하면 `사용법`이라고 보내줘",
    "AI 답변을 생성할 수 없어",
    "타임아웃됐어",
    "권한이 필요해",
    "승인이 필요해",
)


@dataclass
class VerificationResult:
    example: str
    ok: bool
    duration_ms: int
    reply_count: int
    first_line: str
    detail: str


class DummyClient:
    def __init__(self) -> None:
        self.dm_messages: list[dict[str, Any]] = []

    def conversations_replies(self, **_: Any) -> dict[str, Any]:
        return {"messages": []}

    def conversations_open(self, **_: Any) -> dict[str, Any]:
        return {"channel": {"id": "D_USAGE_VERIFY"}}

    def chat_postMessage(self, **kwargs: Any) -> dict[str, Any]:
        self.dm_messages.append(kwargs)
        return {"ok": True, "channel": kwargs.get("channel")}

    def chat_getPermalink(self, **kwargs: Any) -> dict[str, Any]:
        channel = kwargs.get("channel") or "C_USAGE_VERIFY"
        message_ts = kwargs.get("message_ts") or "0"
        return {"permalink": f"https://example.invalid/{channel}/{message_ts}"}

    def users_info(self, **_: Any) -> dict[str, Any]:
        return {
            "user": {
                "name": "usage-verify",
                "real_name": "usage-verify",
                "profile": {"display_name": "usage-verify"},
            }
        }


def _extract_usage_examples() -> list[str]:
    examples: list[str] = []
    for line in _build_usage_help_response().splitlines():
        matched = _BULLET_CODE_LINE_PATTERN.match(line.strip())
        if matched:
            examples.append(matched.group(1).replace(PUBLIC_EXAMPLE_BARCODE, VERIFY_LIVE_BARCODE))
    return examples


def _seed_request_log_examples() -> None:
    VERIFY_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if VERIFY_DB_PATH.exists():
        VERIFY_DB_PATH.unlink()
    _initialize_request_log_storage(db_path=VERIFY_DB_PATH)

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    yesterday_utc = now_utc - timedelta(days=1)

    sample_records = [
        (now_utc, "usage_help", "guide", VERIFY_USER_ID, "usage-verify", "사용법"),
        (now_utc, "db_query", None, VERIFY_USER_ID, "usage-verify", "db 조회 select 1"),
        (now_utc, "barcode_log_analysis", None, VERIFY_USER_ID, "usage-verify", f"{VERIFY_LIVE_BARCODE} 로그 분석"),
        (now_utc, "request log query", "recent", VERIFY_USER_ID, "usage-verify", "요청 로그 최근 20"),
        (yesterday_utc, "ping", None, "U_OTHER", "other-user", "ping"),
        (yesterday_utc, "notion_playbook_qa", None, "U_OTHER", "other-user", "마미박스 동기화 안 될 때 조치"),
    ]

    for index, (created_at_utc, route_name, route_mode, user_id, user_name, request_text) in enumerate(
        sample_records,
        start=1,
    ):
        message_ts = f"1900000000.000{index:03d}"
        _save_request_log_record(
            {
                "createdAtUtc": created_at_utc,
                "sourcePlatform": "slack",
                "workspaceId": "T_USAGE_VERIFY",
                "eventType": "app_mention",
                "routeName": route_name,
                "routeMode": route_mode,
                "status": "handled",
                "userId": user_id,
                "userName": user_name,
                "channelId": "C_USAGE_VERIFY",
                "threadId": message_ts,
                "messageId": message_ts,
                "isThreadRoot": 1,
                "requestText": request_text,
                "normalizedQuestion": request_text,
                "replyCount": 1,
            },
            db_path=VERIFY_DB_PATH,
        )


def _patch_side_effects() -> None:
    company_slack._create_mda_activity_log = lambda _: {"ok": True}  # type: ignore[assignment]

    def fake_download_device_files_to_s3(host: str, port: int, remote_files: list[str]) -> dict[str, Any]:
        return {
            "ok": True,
            "host": host,
            "port": port,
            "downloads": [
                {
                    "ok": True,
                    "fileName": Path(remote_file).name,
                    "url": f"https://example.invalid/download/{Path(remote_file).name}",
                }
                for remote_file in remote_files
            ],
        }

    device_file_probe._download_device_files_to_s3 = fake_download_device_files_to_s3  # type: ignore[assignment]


def _install_request_log_capture() -> list[dict[str, Any]]:
    captured: list[dict[str, Any]] = []
    original_persist = common_slack._persist_request_log

    def capture_persist(payload: Any, *, event_type: str, client: Any, logger: Any) -> None:
        context = payload.get("request_log") if isinstance(payload, dict) else {}
        captured.append(
            {
                "event_type": event_type,
                "route_name": str((context or {}).get("route_name") or ""),
                "route_mode": str((context or {}).get("route_mode") or ""),
                "status": str((context or {}).get("status") or ""),
                "question": str(payload.get("question") or payload.get("raw_text") or ""),
            }
        )
        return original_persist(payload, event_type=event_type, client=client, logger=logger)

    common_slack._persist_request_log = capture_persist  # type: ignore[assignment]
    return captured


def _is_failure(example: str, reply_text: str) -> bool:
    if example == "ping":
        return "pong" not in reply_text.lower()
    return any(token in reply_text for token in _FAIL_TEXT_TOKENS)


def _verify_example(mention_listener: Any, example: str, client: DummyClient, ts_suffix: int) -> VerificationResult:
    replies: list[dict[str, Any]] = []

    def say(**kwargs: Any) -> None:
        replies.append(kwargs)

    event = {
        "text": f"<@U_BOT> {example}",
        "user": VERIFY_USER_ID,
        "channel": "C_USAGE_VERIFY",
        "ts": f"1710000000.{ts_suffix:06d}",
        "thread_ts": f"1710000000.{ts_suffix:06d}",
        "team": "T_USAGE_VERIFY",
    }

    started_at = time.monotonic()
    mention_listener(event=event, say=say, client=client)
    duration_ms = int((time.monotonic() - started_at) * 1000)

    if not replies:
        return VerificationResult(
            example=example,
            ok=False,
            duration_ms=duration_ms,
            reply_count=0,
            first_line="",
            detail="응답이 비어 있어",
        )

    combined_text = "\n\n".join(str(item.get("text") or "") for item in replies)
    first_line = str(combined_text.splitlines()[0] if combined_text.splitlines() else "").strip()
    failed = _is_failure(example, combined_text)
    detail = "ok"
    if failed:
        detail = combined_text.splitlines()[0].strip() or "실패 응답"
    return VerificationResult(
        example=example,
        ok=not failed,
        duration_ms=duration_ms,
        reply_count=len(replies),
        first_line=first_line,
        detail=detail,
    )


def main() -> int:
    _seed_request_log_examples()
    _patch_side_effects()
    captured_logs = _install_request_log_capture()

    app = company_slack.create_app()
    mention_listener = app._listeners[0].ack_function
    client = DummyClient()

    examples = _extract_usage_examples()
    results = [
        _verify_example(mention_listener, example, client, index)
        for index, example in enumerate(examples, start=1)
    ]

    success_count = sum(1 for item in results if item.ok)
    failure_count = len(results) - success_count

    print(f"verified_examples={len(results)} pass={success_count} fail={failure_count}")
    print(f"request_log_db={VERIFY_DB_PATH}")
    print(f"llm_provider={s.LLM_PROVIDER or '(empty)'} synthesis_enabled={s.LLM_SYNTHESIS_ENABLED}")
    print(f"device_file_recovery_enabled={cs.DEVICE_FILE_RECOVERY_ENABLED}")
    print("")

    for item in results:
        status = "PASS" if item.ok else "FAIL"
        print(f"{status} | {item.duration_ms:>5}ms | {item.example}")
        print(f"  first_line: {item.first_line}")
        if not item.ok:
            print(f"  detail: {item.detail}")

    print("")
    print(f"captured_request_log_events={len(captured_logs)} dm_messages={len(client.dm_messages)}")
    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
