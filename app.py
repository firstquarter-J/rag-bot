import json
import logging
import os
import re
from typing import Any
from urllib import error, request

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
MODEL_OWNER_USER_ID = "U0629HDSJHG"
DD_USER_ID = "U0A079J3L9M"
MARK_USER_ID = "U02LBHACKEU"
COMMON_SYSTEM_PROMPT = (
    "너는 마미박스 CS 어시스턴트 Boxer다. "
    "항상 한국어로 답해라. "
    "근거가 부족하면 모른다고 답하고 추측하지 마라. "
    "사실이 불확실한 값(버전, 스펙, 가격, 정책 등)은 단정하지 마라."
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

    if missing:
        raise RuntimeError(
            "필수 환경변수가 설정되지 않았습니다(.env 확인): "
            + ", ".join(missing)
            + ". .env 값을 실제 값으로 교체하세요."
        )


def _extract_question(text: str) -> str:
    return re.sub(r"<@[^>]+>", "", text).strip()


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
    def handle_app_mention(event: dict[str, Any], say) -> None:
        raw_text = event.get("text") or ""
        text = raw_text.lower()
        user_id = event.get("user")
        thread_ts = event.get("thread_ts") or event.get("ts")
        logger.info("Received app_mention: user=%s text=%s", user_id, text)

        special_responses = {
            DD_USER_ID: "간식 통제",
            MARK_USER_ID: "득남 축하",
        }
        special_text = special_responses.get(user_id)
        if special_text:
            say(text=special_text, thread_ts=thread_ts)
            logger.info("Responded with special rule in thread_ts=%s", thread_ts)
            return

        if "ping" in text:
            say(text="pong-ec2", thread_ts=thread_ts)
            logger.info("Responded with pong-ec2 in thread_ts=%s", thread_ts)
            return

        if LLM_PROVIDER == "claude" and claude_client:
            question = _extract_question(raw_text)
            if not question:
                say(text="질문 내용을 같이 보내줘", thread_ts=thread_ts)
                return
            if user_id != MODEL_OWNER_USER_ID:
                say(
                    text="Claude 질문은 현재 지정된 사용자만 사용할 수 있어",
                    thread_ts=thread_ts,
                )
                logger.info("Rejected claude call for user=%s", user_id)
                return
            try:
                answer = _ask_claude(claude_client, question)
                if not answer:
                    answer = "답변을 생성하지 못했어. 다시 질문해줘"
                say(text=answer, thread_ts=thread_ts)
                logger.info("Responded with claude answer in thread_ts=%s", thread_ts)
            except Exception:
                logger.exception("Claude API call failed")
                say(
                    text="AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                    thread_ts=thread_ts,
                )
            return

        if LLM_PROVIDER == "ollama":
            question = _extract_question(raw_text)
            if not question:
                say(text="질문 내용을 같이 보내줘", thread_ts=thread_ts)
                return
            try:
                answer = _ask_ollama(question)
                if not answer:
                    answer = "답변을 생성하지 못했어. 다시 질문해줘"
                say(text=answer, thread_ts=thread_ts)
                logger.info("Responded with ollama answer in thread_ts=%s", thread_ts)
            except Exception:
                logger.exception("Ollama API call failed")
                say(
                    text="Ollama 응답 중 오류가 발생했어. 서버 연결 상태를 확인해줘",
                    thread_ts=thread_ts,
                )
            return

        say(
            text="현재는 ping 또는 LLM 질문에 응답해",
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
