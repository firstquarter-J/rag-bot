import logging
from typing import Any

from boxer.adapters.common.slack import MessagePayload, SlackMessageReplyFn
from boxer.company import settings as cs

ALLOWED_FUN_CHANNEL_ID = "C0621TL2HSB"


def handle_fun_message(
    payload: MessagePayload,
    reply: SlackMessageReplyFn,
    _client: Any,
    logger: logging.Logger,
) -> None:
    if payload["channel_id"] != ALLOWED_FUN_CHANNEL_ID:
        return

    text = payload["text"]
    if "모대" not in text:
        return

    if cs.DD_USER_ID:
        reply(f"<@{cs.DD_USER_ID}> 또 모대?")
    else:
        reply("또 모대?")

    logger.info(
        "Responded with fun trigger in channel=%s user=%s",
        payload["channel_id"],
        payload["user_id"],
    )
