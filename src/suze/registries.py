"""Strategy registries with optional parameter schemas and I/O type hints."""
import logging

from dataclasses import dataclass
from typing import Callable, Optional, Type
from pydantic import BaseModel

logger = logging.getLogger(__name__)


@dataclass
class StrategySpec:
    handler: Callable
    params_model: Optional[Type[BaseModel]] = None
    input_type: Optional[type] = None
    output_type: Optional[type] = None


class Registry:
    def __init__(self):
        self._specs: dict[str, StrategySpec] = {}

    def register(
        self,
        name: str,
        handler: Callable,
        *,
        params_model: Optional[Type[BaseModel]] = None,
    ) -> None:
        ant = handler.__annotations__
        input_type = ant.get("data", list)
        output_type = ant.get("return", list)
        self._specs[name] = StrategySpec(handler, params_model, input_type, output_type)

    def get(self, name: str) -> Optional[Callable]:
        spec = self._specs.get(name)
        return spec.handler if spec else None

    def spec(self, name: str) -> Optional[StrategySpec]:
        return self._specs.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._specs

    def names(self):
        return list(self._specs)
