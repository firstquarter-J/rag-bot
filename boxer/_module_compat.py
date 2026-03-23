from importlib import import_module
from types import ModuleType
from typing import Any


def _reexport_module(module_name: str, namespace: dict[str, Any]) -> ModuleType:
    module = import_module(module_name)
    exported = {
        name: getattr(module, name)
        for name in dir(module)
        if not (name.startswith("__") and name.endswith("__"))
    }
    namespace.update(exported)
    namespace["__doc__"] = getattr(module, "__doc__", None)
    namespace["__all__"] = getattr(
        module,
        "__all__",
        [name for name in exported if not name.startswith("_")],
    )

    def __getattr__(name: str) -> Any:
        return getattr(module, name)

    def __dir__() -> list[str]:
        return sorted(set(namespace) | set(dir(module)))

    namespace["__getattr__"] = __getattr__
    namespace["__dir__"] = __dir__
    return module
