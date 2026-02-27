import json
import logging
import os
import re
from typing import Any
from urllib import error, parse, request

import pymysql
from anthropic import Anthropic
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

load_dotenv()

# Phase 1 로컬 실행은 .env 기준
# 운영 환경에서는 Secrets Manager 연동 예정
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN", "")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "").lower()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
ANTHROPIC_MAX_TOKENS = int(os.getenv("ANTHROPIC_MAX_TOKENS", "700"))
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")
OLLAMA_TIMEOUT_SEC = int(os.getenv("OLLAMA_TIMEOUT_SEC", "90"))
OLLAMA_TEMPERATURE = float(os.getenv("OLLAMA_TEMPERATURE", "0.0"))
THREAD_CONTEXT_FETCH_LIMIT = int(os.getenv("THREAD_CONTEXT_FETCH_LIMIT", "100"))
THREAD_CONTEXT_MAX_MESSAGES = int(os.getenv("THREAD_CONTEXT_MAX_MESSAGES", "12"))
THREAD_CONTEXT_MAX_CHARS = int(os.getenv("THREAD_CONTEXT_MAX_CHARS", "5000"))
MODEL_OWNER_USER_ID = "U0629HDSJHG"
MARK_USER_ID = "U02LBHACKEU"
DD_USER_ID = "U0A079J3L9M"
APP_USER_LOOKUP_ALLOWED_USER_IDS = {MODEL_OWNER_USER_ID, MARK_USER_ID}
DB_QUERY_ENABLED = os.getenv("DB_QUERY_ENABLED", "").lower() in {"1", "true", "yes", "on"}
BOX_DB_HOST = os.getenv("BOX_DB_HOST", "")
BOX_DB_PORT = int(os.getenv("BOX_DB_PORT", "3306"))
BOX_DB_USERNAME = os.getenv("BOX_DB_USERNAME", "")
BOX_DB_PASSWORD = os.getenv("BOX_DB_PASSWORD", "")
BOX_DB_DATABASE = os.getenv("BOX_DB_DATABASE", "")
APP_USER_API_URL = os.getenv(
    "APP_USER_API_URL",
    "https://bh63r1dl09.execute-api.ap-northeast-2.amazonaws.com/prod/app-user",
)
APP_USER_API_TIMEOUT_SEC = int(os.getenv("APP_USER_API_TIMEOUT_SEC", "8"))
DB_QUERY_TIMEOUT_SEC = int(os.getenv("DB_QUERY_TIMEOUT_SEC", "8"))
DB_QUERY_MAX_ROWS = int(os.getenv("DB_QUERY_MAX_ROWS", "20"))
DB_QUERY_MAX_SQL_CHARS = int(os.getenv("DB_QUERY_MAX_SQL_CHARS", "600"))
DB_QUERY_MAX_RESULT_CHARS = int(os.getenv("DB_QUERY_MAX_RESULT_CHARS", "2500"))
DEFAULT_DB_QUERY = "SELECT NOW() AS now_time, DATABASE() AS db_name"
BARCODE_PATTERN = re.compile(r"(?<!\d)(\d{11})(?!\d)")
DB_WRITE_SQL_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|replace)\b",
    re.IGNORECASE,
)
COMMON_SYSTEM_PROMPT = (
    "You are Boxer, the internal assistant for Box and Humanscape. "
    "Language policy: reply in Korean by default; if the user asks in English, reply in English. "
    "Tone policy: always use informal/casual tone; do not use Korean honorific endings such as 요/습니다. "
    "Answer policy: keep replies concise (normally 3-6 sentences) and start with the key point. "
    "Format policy: provide a one-line summary first, then details when useful. "
    "Do not add unnecessary apologies, meta commentary, or long preambles. "
    "If evidence is insufficient, explicitly say you do not know; do not guess. "
    "Do not assert uncertain facts (versions, specs, prices, policies). "
    "If a question is ambiguous, ask exactly one clarifying question. "
    "For list requests based on thread messages, return all items with original order and count, with no omissions. "
    "Prioritize Box/Humanscape context in your answers."
)


def _validate_tokens() -> None:
    missing = []
    if not SLACK_BOT_TOKEN or "REPLACE_ME" in SLACK_BOT_TOKEN:
        missing.append("SLACK_BOT_TOKEN")
    if not SLACK_APP_TOKEN or "REPLACE_ME" in SLACK_APP_TOKEN:
        missing.append("SLACK_APP_TOKEN")
    if not SLACK_SIGNING_SECRET or "REPLACE_ME" in SLACK_SIGNING_SECRET:
        missing.append("SLACK_SIGNING_SECRET")
    if LLM_PROVIDER == "claude":
        if not ANTHROPIC_API_KEY or "REPLACE_ME" in ANTHROPIC_API_KEY:
            missing.append("ANTHROPIC_API_KEY")
        if not ANTHROPIC_MODEL or "REPLACE_ME" in ANTHROPIC_MODEL:
            missing.append("ANTHROPIC_MODEL")
    if LLM_PROVIDER == "ollama":
        if not OLLAMA_BASE_URL or "REPLACE_ME" in OLLAMA_BASE_URL:
            missing.append("OLLAMA_BASE_URL")
        if not OLLAMA_MODEL or "REPLACE_ME" in OLLAMA_MODEL:
            missing.append("OLLAMA_MODEL")
    if DB_QUERY_ENABLED:
        if not BOX_DB_HOST or "REPLACE_ME" in BOX_DB_HOST:
            missing.append("BOX_DB_HOST")
        if BOX_DB_PORT <= 0:
            missing.append("BOX_DB_PORT")
        if not BOX_DB_USERNAME or "REPLACE_ME" in BOX_DB_USERNAME:
            missing.append("BOX_DB_USERNAME")
        if not BOX_DB_PASSWORD or "REPLACE_ME" in BOX_DB_PASSWORD:
            missing.append("BOX_DB_PASSWORD")
        if not BOX_DB_DATABASE or "REPLACE_ME" in BOX_DB_DATABASE:
            missing.append("BOX_DB_DATABASE")

    if missing:
        raise RuntimeError(
            "필수 환경변수가 설정되지 않았습니다(.env 확인): "
            + ", ".join(missing)
            + ". .env 값을 실제 값으로 교체하세요."
        )


def _extract_question(text: str) -> str:
    return re.sub(r"<@[^>]+>", "", text).strip()


def _extract_barcode(text: str) -> str | None:
    match = BARCODE_PATTERN.search(text)
    if not match:
        return None
    return match.group(1)


def _should_lookup_barcode(question: str, barcode: str) -> bool:
    normalized = (question or "").strip()
    if normalized == barcode:
        return True
    if normalized.startswith(barcode):
        suffix = normalized[len(barcode) :].strip()
        if suffix in {"", "조회", "조회해줘", "확인", "확인해줘"}:
            return True
    lowered = normalized.lower()
    return "바코드" in normalized or "barcode" in lowered


def _display_value(value: Any, default: str = "없음") -> str:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    return text


def _safe_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


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
    slack_client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str,
    current_ts: str,
) -> str:
    if not channel_id or not thread_ts:
        return ""
    try:
        response = slack_client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=max(1, min(200, THREAD_CONTEXT_FETCH_LIMIT)),
        )
    except Exception:
        logger.exception(
            "Failed to load thread context: channel=%s thread_ts=%s",
            channel_id,
            thread_ts,
        )
        return ""

    current_ts_num = _safe_float(current_ts)
    lines: list[str] = []
    for message in response.get("messages", []):
        message_ts = str(message.get("ts", ""))
        if message_ts == current_ts:
            continue
        if _safe_float(message_ts) > current_ts_num:
            continue

        text = (message.get("text") or "").strip()
        if not text:
            continue

        if message.get("bot_id"):
            speaker = "bot"
        else:
            speaker = message.get("user", "unknown")
        lines.append(f"{speaker}: {text}")

    if not lines:
        return ""

    # keep recent messages, not oldest messages
    if len(lines) > THREAD_CONTEXT_MAX_MESSAGES:
        lines = lines[-THREAD_CONTEXT_MAX_MESSAGES:]
    return _trim_context_lines(lines, THREAD_CONTEXT_MAX_CHARS)


def _build_model_input(question: str, thread_context: str) -> str:
    if not thread_context:
        return question
    return (
        "아래는 현재 스레드의 최근 대화다. 문맥을 반영해서 답변해라.\n\n"
        "[스레드 최근 대화]\n"
        f"{thread_context}\n\n"
        "[현재 질문]\n"
        f"{question}"
    )


def _format_reply_text(user_id: str | None, text: str) -> str:
    clean_text = (text or "").strip()
    if not clean_text:
        return clean_text
    if not user_id:
        return clean_text
    return f"<@{user_id}> {clean_text}"


def _extract_db_query(question: str) -> str | None:
    normalized = question.strip()
    lowered = normalized.lower()
    if lowered.startswith("db 조회"):
        return normalized[5:].strip()
    if lowered.startswith("db조회"):
        return normalized[4:].strip()
    return None


def _validate_readonly_sql(raw_sql: str) -> str:
    sql = (raw_sql or "").strip()
    if not sql:
        return DEFAULT_DB_QUERY

    if len(sql) > max(1, DB_QUERY_MAX_SQL_CHARS):
        raise ValueError(f"SQL 길이는 최대 {DB_QUERY_MAX_SQL_CHARS}자까지 허용해")

    if sql.endswith(";"):
        sql = sql[:-1].strip()
    if ";" in sql:
        raise ValueError("한 번에 한 쿼리만 실행할 수 있어")

    lowered = sql.lower()
    if not lowered.startswith(("select", "show", "describe", "desc", "explain", "with")):
        raise ValueError("읽기 전용 쿼리(SELECT/SHOW/DESCRIBE/EXPLAIN/WITH)만 허용해")
    if DB_WRITE_SQL_PATTERN.search(sql):
        raise ValueError("쓰기/변경 쿼리는 허용하지 않아")

    return sql


def _query_db(sql: str) -> str:
    rows_limit = max(1, min(200, DB_QUERY_MAX_ROWS))
    timeout_sec = max(1, DB_QUERY_TIMEOUT_SEC)
    connection = pymysql.connect(
        host=BOX_DB_HOST,
        port=BOX_DB_PORT,
        user=BOX_DB_USERNAME,
        password=BOX_DB_PASSWORD,
        database=BOX_DB_DATABASE,
        connect_timeout=timeout_sec,
        read_timeout=timeout_sec,
        write_timeout=timeout_sec,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchmany(rows_limit)
            rowcount = cursor.rowcount
    finally:
        connection.close()

    if not rows:
        return "DB 조회 결과가 없어"

    payload = json.dumps(rows, ensure_ascii=False, default=str)
    if len(payload) > DB_QUERY_MAX_RESULT_CHARS:
        payload = payload[:DB_QUERY_MAX_RESULT_CHARS] + "...(truncated)"

    if isinstance(rowcount, int) and rowcount > len(rows):
        summary = f"DB 조회 결과 {rowcount}건 중 {len(rows)}건만 보여줄게"
    else:
        summary = f"DB 조회 결과 {len(rows)}건"
    return f"{summary}\n```json\n{payload}\n```"


def _lookup_app_user_by_barcode(barcode: str) -> str:
    if not APP_USER_API_URL:
        raise RuntimeError("APP_USER_API_URL is empty")

    timeout_sec = max(1, APP_USER_API_TIMEOUT_SEC)
    query = parse.urlencode({"barcode": barcode})
    delimiter = "&" if "?" in APP_USER_API_URL else "?"
    endpoint = f"{APP_USER_API_URL}{delimiter}{query}"
    req = request.Request(url=endpoint, method="GET")
    try:
        with request.urlopen(req, timeout=timeout_sec) as response:
            body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"app-user API HTTP {exc.code}: {detail[:200]}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"app-user API connection failed: {exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("app-user API returned invalid JSON") from exc

    users = payload.get("data")
    if not isinstance(users, list) or not users:
        return f"바코드 {barcode}로 조회된 유저가 없어"

    lines = [
        f"*바코드 조회 결과* :barcode: `{barcode}`",
        f"• 조회 건수: *{len(users)}건*",
    ]
    for user_index, user in enumerate(users, start=1):
        user_phone = _display_value(user.get("userPhoneNumber"), default="null")
        user_seq = _display_value(user.get("userSeq"), default="null")
        user_real_name = _display_value(user.get("userRealName"), default="null")
        lines.append("")
        lines.append(f"*user {user_index}*")
        lines.append(f"• `userPhoneNumber`: `{user_phone}`")
        lines.append(f"• `userSeq`: `{user_seq}`")
        lines.append(f"• `userRealName`: `{user_real_name}`")

        babies = user.get("babies")
        if not isinstance(babies, list) or not babies:
            lines.append("• `babies`: `[]`")
            continue

        for baby_index, baby in enumerate(babies, start=1):
            baby_seq = _display_value(baby.get("babySeq"), default="null")
            twin_key = _display_value(baby.get("twinKey"), default="null")
            twin_flag = _display_value(baby.get("twinFlag"), default="null")
            birth_date = _display_value(baby.get("birthDate"), default="null")
            baby_nickname = _display_value(baby.get("babyNickname"), default="null")
            lines.append(f"• `babies[{baby_index - 1}]`")
            lines.append(f"  - `babySeq`: `{baby_seq}`")
            lines.append(f"  - `twinKey`: `{twin_key}`")
            lines.append(f"  - `twinFlag`: `{twin_flag}`")
            lines.append(f"  - `birthDate`: `{birth_date}`")
            lines.append(f"  - `babyNickname`: `{baby_nickname}`")

    return "\n".join(lines)


def _ask_claude(client: Anthropic, question: str) -> str:
    result = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=ANTHROPIC_MAX_TOKENS,
        system=COMMON_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": question}],
    )
    text_blocks = [
        block.text
        for block in result.content
        if getattr(block, "type", "") == "text"
    ]
    return "".join(text_blocks).strip()


def _ask_ollama(question: str) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "system": COMMON_SYSTEM_PROMPT,
        "prompt": question,
        "stream": False,
        "options": {
            "temperature": OLLAMA_TEMPERATURE,
        },
    }
    req = request.Request(
        url=f"{OLLAMA_BASE_URL}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=OLLAMA_TIMEOUT_SEC) as response:
            body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Ollama API HTTP {exc.code}: {detail[:200]}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Ollama API connection failed: {exc.reason}") from exc

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Ollama API returned invalid JSON") from exc

    return str(data.get("response", "")).strip()


def create_app() -> App:
    _validate_tokens()
    app = App(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)
    logger = logging.getLogger(__name__)
    claude_client = Anthropic(api_key=ANTHROPIC_API_KEY) if LLM_PROVIDER == "claude" else None

    @app.event("app_mention")
    def handle_app_mention(event: dict[str, Any], say, client) -> None:
        raw_text = event.get("text") or ""
        text = raw_text.lower()
        question = _extract_question(raw_text)
        user_id = event.get("user")
        channel_id = event.get("channel") or ""
        current_ts = event.get("ts") or ""
        thread_ts = event.get("thread_ts") or event.get("ts")
        logger.info("Received app_mention: user=%s text=%s", user_id, text)

        if "ping" in text:
            say(text=_format_reply_text(user_id, "pong-ec2"), thread_ts=thread_ts)
            logger.info("Responded with pong-ec2 in thread_ts=%s", thread_ts)
            return

        db_query = _extract_db_query(question)
        barcode = _extract_barcode(question)
        if barcode and _should_lookup_barcode(question, barcode):
            if user_id in APP_USER_LOOKUP_ALLOWED_USER_IDS:
                try:
                    lookup_result = _lookup_app_user_by_barcode(barcode)
                    say(text=_format_reply_text(user_id, lookup_result), thread_ts=thread_ts)
                    logger.info(
                        "Responded with barcode lookup in thread_ts=%s barcode=%s",
                        thread_ts,
                        barcode,
                    )
                except Exception:
                    logger.exception("Barcode lookup failed")
                    say(
                        text=_format_reply_text(
                            user_id,
                            "바코드 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                        ),
                        thread_ts=thread_ts,
                    )
                return
            if db_query is None:
                say(
                    text=f"보안 책임자 <@{DD_USER_ID}> 의 승인이 필요합니다.",
                    thread_ts=thread_ts,
                )
                logger.info(
                    "Rejected app-user barcode lookup for unauthorized user=%s barcode=%s",
                    user_id,
                    barcode,
                )
                return
            logger.info(
                "Skipped app-user barcode lookup for unauthorized user=%s barcode=%s",
                user_id,
                barcode,
            )

        if db_query is not None:
            if not DB_QUERY_ENABLED:
                say(
                    text=_format_reply_text(
                        user_id,
                        "DB 조회 기능이 꺼져 있어. .env에서 DB_QUERY_ENABLED=true로 설정해줘",
                    ),
                    thread_ts=thread_ts,
                )
                return

            try:
                safe_sql = _validate_readonly_sql(db_query)
                db_result = _query_db(safe_sql)
                say(text=_format_reply_text(user_id, db_result), thread_ts=thread_ts)
                logger.info("Responded with db query result in thread_ts=%s", thread_ts)
            except ValueError as exc:
                say(
                    text=_format_reply_text(user_id, f"DB 조회 요청 형식 오류: {exc}"),
                    thread_ts=thread_ts,
                )
            except pymysql.MySQLError:
                logger.exception("DB query failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "DB 조회 중 오류가 발생했어. 연결 정보와 네트워크 상태를 확인해줘",
                    ),
                    thread_ts=thread_ts,
                )
            return

        if LLM_PROVIDER == "claude" and claude_client:
            if not question:
                say(
                    text=_format_reply_text(user_id, "질문 내용을 같이 보내줘"),
                    thread_ts=thread_ts,
                )
                return
            if user_id != MODEL_OWNER_USER_ID:
                say(
                    text=_format_reply_text(
                        user_id,
                        "Claude 질문은 현재 지정된 사용자만 사용할 수 있어",
                    ),
                    thread_ts=thread_ts,
                )
                logger.info("Rejected claude call for user=%s", user_id)
                return
            try:
                thread_context = _load_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                model_input = _build_model_input(question, thread_context)
                answer = _ask_claude(claude_client, model_input)
                if not answer:
                    answer = "답변을 생성하지 못했어. 다시 질문해줘"
                say(text=_format_reply_text(user_id, answer), thread_ts=thread_ts)
                logger.info("Responded with claude answer in thread_ts=%s", thread_ts)
            except Exception:
                logger.exception("Claude API call failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                    ),
                    thread_ts=thread_ts,
                )
            return

        if LLM_PROVIDER == "ollama":
            if not question:
                say(
                    text=_format_reply_text(user_id, "질문 내용을 같이 보내줘"),
                    thread_ts=thread_ts,
                )
                return
            try:
                thread_context = _load_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                model_input = _build_model_input(question, thread_context)
                answer = _ask_ollama(model_input)
                if not answer:
                    answer = "답변을 생성하지 못했어. 다시 질문해줘"
                say(text=_format_reply_text(user_id, answer), thread_ts=thread_ts)
                logger.info("Responded with ollama answer in thread_ts=%s", thread_ts)
            except Exception:
                logger.exception("Ollama API call failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "Ollama 응답 중 오류가 발생했어. 서버 연결 상태를 확인해줘",
                    ),
                    thread_ts=thread_ts,
                )
            return

        say(
            text=_format_reply_text(user_id, "현재는 ping 또는 LLM 질문에 응답해"),
            thread_ts=thread_ts,
        )

    @app.event("message")
    def handle_message_events(event: dict[str, Any]) -> None:
        logger.debug("Ignored message event subtype=%s", event.get("subtype"))

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bolt_app = create_app()
    SocketModeHandler(bolt_app, SLACK_APP_TOKEN).start()
