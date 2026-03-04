import importlib
from collections.abc import Callable
from typing import Any

from boxer.core import settings as s


def load_entrypoint(path: str) -> Callable[[], Any]:
    target = (path or "").strip()
    if ":" not in target:
        raise RuntimeError(
            "ADAPTER_ENTRYPOINT 형식 오류: '<module>:<callable>' 형식이어야 해"
        )

    module_name, callable_name = target.split(":", 1)
    module_name = module_name.strip()
    callable_name = callable_name.strip()
    if not module_name or not callable_name:
        raise RuntimeError(
            "ADAPTER_ENTRYPOINT 형식 오류: '<module>:<callable>' 형식이어야 해"
        )

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise RuntimeError(f"어댑터 모듈 로딩 실패: {module_name}") from exc

    factory = getattr(module, callable_name, None)
    if not callable(factory):
        raise RuntimeError(f"어댑터 팩토리 함수가 없어: {target}")

    return factory


def create_app() -> Any:
    factory = load_entrypoint(s.ADAPTER_ENTRYPOINT)
    return factory()


# legacy alias
_load_entrypoint = load_entrypoint
