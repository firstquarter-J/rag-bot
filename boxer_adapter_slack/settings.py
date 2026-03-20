import os

from boxer.core import settings as _core_settings

# `boxer.core.settings`가 dotenv 로딩을 담당하므로 import side effect를 유지한다.
_ = _core_settings.PROJECT_ROOT

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN", "")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")
ADAPTER_ENTRYPOINT = os.getenv("ADAPTER_ENTRYPOINT", "boxer_adapter_slack.sample:create_app")


def validate_slack_tokens() -> None:
    missing: list[str] = []
    if not SLACK_BOT_TOKEN or "REPLACE_ME" in SLACK_BOT_TOKEN:
        missing.append("SLACK_BOT_TOKEN")
    if not SLACK_APP_TOKEN or "REPLACE_ME" in SLACK_APP_TOKEN:
        missing.append("SLACK_APP_TOKEN")
    if not SLACK_SIGNING_SECRET or "REPLACE_ME" in SLACK_SIGNING_SECRET:
        missing.append("SLACK_SIGNING_SECRET")
    if missing:
        raise RuntimeError(
            "필수 Slack 환경변수가 설정되지 않았습니다(.env 확인): "
            + ", ".join(missing)
            + ". .env 값을 실제 값으로 교체하세요."
        )
