"""Legacy adapter alias.

기존 `boxer.adapters.slack:create_app` 경로 호환을 위해 유지한다.
실제 로딩은 ADAPTER_ENTRYPOINT를 따르며, self-pointing 설정 시 재귀를 방지한다.
"""

from typing import Any

from boxer.adapters.factory import load_entrypoint
from boxer.core import settings as s


def create_app() -> Any:
    target = (s.ADAPTER_ENTRYPOINT or "").strip()
    if target in {"", "boxer.adapters.slack:create_app"}:
        target = "boxer.adapters.sample.slack:create_app"
    factory = load_entrypoint(target)
    return factory()


__all__ = ["create_app"]
