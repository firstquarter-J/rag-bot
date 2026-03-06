import logging
import random
import re
from typing import Any

from boxer.adapters.common.slack import MessagePayload, SlackMessageReplyFn
from boxer.company import settings as cs

ALLOWED_FUN_CHANNEL_ID = "C0621TL2HSB"
GENERIC_FUN_REPLIES = (
    "또 모대?",
    "모대 또 왔네?",
    "모대냐 또?",
    "또 모대 타임이야?",
    "모대 모대 또 모대?",
    "모대? 오늘도 모대?",
)
TOPIC_FUN_TEMPLATES = (
    '"{topic}"도 모대?',
    '"{topic}" 얘기도 모대?',
    '결국 "{topic}"도 모대?',
    '"{topic}"까지 모대?',
    '이제 "{topic}"도 모대?',
    '"{topic}" 또 모대?',
)
CLAUSE_SPLIT_RE = re.compile(r"[\n\r,.!?~]+")
MENTION_RE = re.compile(r"<@[^>]+>")
URL_RE = re.compile(r"https?://\S+")
EDGE_FILLER_RE = re.compile(r"^(또|진짜|완전|아니|근데|그럼|와|헐)\s+|\s+(또|진짜|완전|아니|근데|그럼|와|헐)$")
TRAILING_ENDING_RE = re.compile(r"(이네|이야|인가요|인가|인데|네요|네요|이냐|이군)$")
TRAILING_PARTICLE_RE = re.compile(r"(은|는|이|가|을|를|도|만|이나|나|랑|과|와|임|야|냐|네|군|지)$")


def _normalize_fun_text(text: str) -> str:
    normalized = MENTION_RE.sub(" ", text)
    normalized = URL_RE.sub(" ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _clean_fun_fragment(text: str) -> str:
    cleaned = text.strip(" \"'[]()")
    cleaned = EDGE_FILLER_RE.sub("", cleaned).strip()
    cleaned = TRAILING_ENDING_RE.sub("", cleaned).strip()
    cleaned = TRAILING_PARTICLE_RE.sub("", cleaned).strip()
    cleaned = EDGE_FILLER_RE.sub("", cleaned).strip()
    return cleaned


def _extract_fun_topic(text: str) -> str | None:
    normalized = _normalize_fun_text(text)
    if "모대" not in normalized:
        return None

    clauses = [segment.strip() for segment in CLAUSE_SPLIT_RE.split(normalized) if segment.strip()]
    clause = next((segment for segment in clauses if "모대" in segment), normalized)
    before, _, after = clause.partition("모대")
    before = _clean_fun_fragment(before)
    after = _clean_fun_fragment(after)

    topic = before or after
    if not topic:
        topic = _clean_fun_fragment(clause.replace("모대", " "))

    if not topic:
        return None

    words = topic.split()
    if len(words) > 4:
        topic = " ".join(words[-4:])
    if len(topic) > 24:
        topic = topic[-24:].strip()
    return topic or None


def _build_fun_reply(text: str) -> str:
    topic = _extract_fun_topic(text)
    if not topic:
        return random.choice(GENERIC_FUN_REPLIES)
    return random.choice(TOPIC_FUN_TEMPLATES).format(topic=topic)


def handle_fun_message(
    payload: MessagePayload,
    reply: SlackMessageReplyFn,
    _client: Any,
    logger: logging.Logger,
) -> None:
    if payload["channel_id"] != ALLOWED_FUN_CHANNEL_ID:
        return

    raw_text = payload["raw_text"]
    if "모대" not in raw_text:
        return

    reply_text = _build_fun_reply(raw_text)
    if cs.DD_USER_ID:
        reply(f"<@{cs.DD_USER_ID}> {reply_text}", thread=True)
    else:
        reply(reply_text, thread=True)

    logger.info(
        "Responded with fun trigger in channel=%s user=%s",
        payload["channel_id"],
        payload["user_id"],
    )
