"""Data quality: pydantic row schemas + optional pandera enforcement.

Used by the `validate` stage. `validate_stage` is the default strategy and
is chosen because it takes the request's `ValidateParams` directly. Users
who need more (cross-field checks, DataFrame-level constraints) register a
custom `validate` strategy with their own `params_model`.
"""

from typing import Any, Optional, Type
from pydantic import BaseModel, ValidationError


class SchemaError(ValueError):
    def __init__(self, errors: list[dict]):
        self.errors = errors
        super().__init__(f"{len(errors)} validation error(s)")


def enforce_rows(
    data: list[dict],
    model: Type[BaseModel],
    *,
    on_error: str = "raise",
) -> list[dict]:
    good, bad = [], []
    for i, row in enumerate(data):
        try:
            good.append(model(**row).model_dump())
        except ValidationError as e:
            bad.append({"row": i, "errors": e.errors()})
    if bad and on_error == "raise":
        raise SchemaError(bad)
    if on_error == "drop":
        return good
    return good


def enforce_pandera(data: list[dict], schema) -> list[dict]:
    try:
        import pandas as pd
    except ImportError as e:
        raise RuntimeError("pandera enforcement requires pandas") from e
    df = pd.DataFrame(data)
    try:
        validated = schema.validate(df)
    except Exception as e:
        raise SchemaError([{"error": str(e)}]) from e
    return validated.to_dict(orient="records")


async def validate_stage(
    data,
    dependencies,
    *,
    model: Optional[Type[BaseModel]] = None,
    pand_schema=None,
    on_error: str = "raise",
    **kwargs,
):
    """Default `validate` handler. Returns data unchanged on success."""
    del dependencies, kwargs
    if model is not None:
        return enforce_rows(data, model, on_error=on_error)
    if schema is not None:
        return enforce_pandera(data, schema)
    return data
