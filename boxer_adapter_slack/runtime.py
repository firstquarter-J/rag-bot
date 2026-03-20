import logging

from slack_bolt.adapter.socket_mode import SocketModeHandler

from boxer_adapter_slack.factory import create_app
from boxer_adapter_slack import settings as ss


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    ss.validate_slack_tokens()
    bolt_app = create_app()
    SocketModeHandler(bolt_app, ss.SLACK_APP_TOKEN).start()


if __name__ == "__main__":
    main()
