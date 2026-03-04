import logging
from typing import Any

from slack_bolt import App

from boxer.adapters.common.slack import MentionPayload, SlackReplyFn, create_slack_app


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
            reply("pong")
            logger.info("Responded with sample pong in thread_ts=%s", payload["thread_ts"])
            return

        if not question:
            reply("질문 내용을 같이 보내줘")
            return

        reply(
            "샘플 어댑터가 동작 중이야. 회사용 기능은 ADAPTER_ENTRYPOINT를 company 어댑터로 바꿔서 사용해줘"
        )

    return create_slack_app(_handle_sample_mention)
