import logging
from typing import Any

from slack_bolt import App

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _set_request_log_route,
    create_slack_app,
)


def create_app() -> App:
    def _handle_sample_mention(
        payload: MentionPayload,
        reply: SlackReplyFn,
        _client: Any,
        logger: logging.Logger,
    ) -> None:
        text = payload["text"]
        question = payload["question"].strip()

        if "ping" in text:
            _set_request_log_route(payload, "sample_ping")
            reply("pong")
            logger.info("Responded with sample pong in thread_ts=%s", payload["thread_ts"])
            return

        if not question:
            _set_request_log_route(payload, "sample_empty_question", status="rejected")
            reply("질문 내용을 같이 보내줘")
            return

        _set_request_log_route(payload, "sample_default")
        reply(
            "샘플 어댑터가 동작 중이야. `ping`으로 확인하거나 custom adapter 예제를 시작점으로 붙이면 돼"
        )

    return create_slack_app(_handle_sample_mention)
