"""Typed stage configuration for DataRequest.

Each stage name has a small pydantic model. Unknown keys are rejected at
construction time, so a typo like {"load": {"stratgy": "json"}} fails
immediately instead of silently falling back to the default strategy.
"""

from typing import Any, Optional, Literal, Callable, Union
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy: Optional[Union[str, Callable]] = None
    parameters: dict[str, Any] = Field(default_factory=dict)


class LoadConfig(StageConfig):
    """Only stage allowed to do I/O."""


class SearchConfig(StageConfig):
    pass


class ValidateParams(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")
    model: Optional[Any] = None           # pydantic row model
    pand_schema: Optional[Any] = None          # pandera DataFrameSchema
    on_error: Literal["raise", "drop", "collect"] = "raise"


class ValidateConfig(StageConfig):
    parameters: ValidateParams = Field(default_factory=ValidateParams)


class TransformConfig(StageConfig):
    pass


class DropConfig(StageConfig):
    pass


class RenameConfig(StageConfig):
    pass


class PipeConfig(StageConfig):
    """`sink` lives here — the only exit point for data."""


STAGE_CONFIGS: dict[str, type[StageConfig]] = {
    "load": LoadConfig,
    "search": SearchConfig,
    "validate": ValidateConfig,
    "transform": TransformConfig,
    "drop": DropConfig,
    "rename": RenameConfig,
    "pipe": PipeConfig,
}


def build_stage_config(name: str, data: dict | StageConfig) -> StageConfig:
    if isinstance(data, StageConfig):
        return data
    cls = STAGE_CONFIGS.get(name, StageConfig)
    return cls(**data)
