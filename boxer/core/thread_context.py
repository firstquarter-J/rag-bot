import logging
from typing import Any

from boxer.core import settings as s
from boxer.core.utils import _safe_float


def _trim_context_lines(lines: list[str], max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    kept: list[str] = []
    total_chars = 0
    for line in reversed(lines):
        next_len = len(line) + (1 if kept else 0)
        if total_chars + next_len > max_chars:
            break
        kept.append(line)
        total_chars += next_len
    kept.reverse()
    return "\n".join(kept)


def _load_thread_context(
    client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str | None,
    current_ts: str | None,
) -> str:
    if not channel_id or not thread_ts:
        return ""

    try:
        replies = client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=max(1, s.THREAD_CONTEXT_FETCH_LIMIT),
            inclusive=True,
        )
    except Exception:
        logger.exception("Failed to fetch thread context")
        return ""

    messages = replies.get("messages") or []
    if not isinstance(messages, list):
        return ""

    filtered: list[dict[str, Any]] = []
    current_ts_float = _safe_float(current_ts or "")
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        msg_ts = _safe_float(msg.get("ts") or "")
        if current_ts and msg_ts >= current_ts_float:
            continue
        filtered.append(msg)

    if not filtered:
        return ""

    max_messages = max(1, s.THREAD_CONTEXT_MAX_MESSAGES)
    trimmed = filtered[-max_messages:]

    lines: list[str] = []
    for msg in trimmed:
        user = (msg.get("user") or "unknown").strip() or "unknown"
        text = (msg.get("text") or "").strip()
        if not text:
            continue
        lines.append(f"{user}: {text}")

    if not lines:
        return ""

    return _trim_context_lines(lines, max(1, s.THREAD_CONTEXT_MAX_CHARS))


def _build_model_input(question: str, thread_context: str) -> str:
    base_question = (question or "").strip()
    if not thread_context:
        return base_question
    return (
        "Thread context (older -> newer):\n"
        f"{thread_context}\n\n"
        "Current user question:\n"
        f"{base_question}"
    )
