import logging
from typing import Any, Callable, Protocol, TypedDict

from slack_bolt import App

from boxer.core import settings as s
from boxer.core.utils import _extract_question, _format_reply_text, _validate_tokens


class MentionPayload(TypedDict):
    raw_text: str
    text: str
    question: str
    user_id: str | None
    channel_id: str
    current_ts: str
    thread_ts: str


class SlackReplyFn(Protocol):
    def __call__(self, text: str, *, mention_user: bool = True) -> None: ...


MentionHandler = Callable[[MentionPayload, SlackReplyFn, Any, logging.Logger], None]


def create_slack_app(mention_handler: MentionHandler) -> App:
    _validate_tokens(include_llm=False, include_data_sources=False)
    app = App(token=s.SLACK_BOT_TOKEN, signing_secret=s.SLACK_SIGNING_SECRET)
    logger = logging.getLogger(__name__)

    @app.event("app_mention")
    def handle_app_mention(event: dict[str, Any], say, client) -> None:
        raw_text = event.get("text") or ""
        text = raw_text.lower()
        user_id = event.get("user")
        thread_ts = event.get("thread_ts") or event.get("ts")

        payload: MentionPayload = {
            "raw_text": raw_text,
            "text": text,
            "question": _extract_question(raw_text),
            "user_id": user_id,
            "channel_id": event.get("channel") or "",
            "current_ts": event.get("ts") or "",
            "thread_ts": thread_ts or "",
        }
        logger.info("Received app_mention: user=%s text=%s", user_id, text)

        def reply(reply_text: str, *, mention_user: bool = True) -> None:
            if mention_user:
                say(text=_format_reply_text(user_id, reply_text), thread_ts=thread_ts)
                return

            clean_text = (reply_text or "").strip()
            if not clean_text:
                clean_text = "응답 내용이 비어 있어"
            say(text=clean_text, thread_ts=thread_ts)

        mention_handler(payload, reply, client, logger)

    @app.event("message")
    def handle_message_events(event: dict[str, Any]) -> None:
        logger.debug("Ignored message event subtype=%s", event.get("subtype"))

    return app
