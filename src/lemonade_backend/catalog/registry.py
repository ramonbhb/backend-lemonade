from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lemonade_backend.executors.base import BaseSparkExecutor


class ExecutorRegistry:
    def __init__(self) -> None:
        self._executors: dict[str, type[BaseSparkExecutor]] = {}

    def register(self, name: str, executor_cls: type[BaseSparkExecutor]) -> None:
        self._executors[name] = executor_cls

    def get(self, name: str) -> type[BaseSparkExecutor] | None:
        return self._executors.get(name)

    def list_names(self) -> list[str]:
        return sorted(self._executors.keys())

    def all(self) -> dict[str, type[BaseSparkExecutor]]:
        return dict(self._executors)


_registry = ExecutorRegistry()


def get_registry() -> ExecutorRegistry:
    return _registry


def register_all_executors() -> ExecutorRegistry:
    from lemonade_backend.executors.classification import CLASSIFICATION_EXECUTORS
    from lemonade_backend.executors.clustering import CLUSTERING_EXECUTORS
    from lemonade_backend.executors.evaluation import EVALUATION_EXECUTORS
    from lemonade_backend.executors.io import IO_EXECUTORS
    from lemonade_backend.executors.regression import REGRESSION_EXECUTORS

    registry = get_registry()
    for group in (
        IO_EXECUTORS,
        CLASSIFICATION_EXECUTORS,
        REGRESSION_EXECUTORS,
        CLUSTERING_EXECUTORS,
        EVALUATION_EXECUTORS,
    ):
        for name, cls in group.items():
            registry.register(name, cls)
    return registry
