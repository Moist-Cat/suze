"""Sink adapters for the `pipe` stage.

`pipe` is the *exit* of the pipeline — the only place data leaves the
framework. Keeping `load` as the only entry and `pipe` as the only exit
means intermediate stages are guaranteed pure in-memory transforms.

Implement `Sink` (duck-typed: `async def write(data, *, request=None)`)
and drop it into `parameters.sink` of the pipe stage.
"""

import asyncio
import aiofiles
import csv
import json
from pathlib import Path
from typing import Any, Callable, Protocol, runtime_checkable, Sequence
from hashlib import md5

class Sink:
    async def write(self, data: Any, *, request=None) -> Any: ...

    def __str__(self):
        return self.__class__.__name__


class NullSink(Sink):
    """Discard. Default when no sink is configured."""
    async def write(self, data, *, request=None):
        return data


class MemorySink(Sink):
    """Collect writes in memory. Useful for tests."""
    def __init__(self):
        self.records: list = []
        self.writes = 0

    async def write(self, data, *, request=None):
        self.records.append(data)
        self.writes += 1
        return data


class CallableSink(Sink):
    """Wrap any sync or async callable as a sink."""
    def __init__(self, fn: Callable):
        self.fn = fn

    async def write(self, data, *, request=None):
        if asyncio.iscoroutinefunction(self.fn):
            await self.fn(data)
        else:
            await asyncio.to_thread(self.fn, data)
        return data

class LiteralSink(Sink):
    """
    Just dump my data.
    """

    def __init__(self, path):
        self.path = Path(path)

    async def write(self, data, *, request=None):
        if not isinstance(data, list):
            data = [data]
        for item in data:
            if isinstance(item, str):
                encoded_data = item.encode()
                mode = "w"
            else:
                encoded_data = item
                mode = "wb"
            try:
                name = md5(encoded_data).hexdigest()
            except TypeError as tp:
                print(encoded_data)
                raise
            async with aiofiles.open(self.path / name, mode=mode, encoding="utf8") as fd:
                await fd.write(item)
        return data


class JSONSink(Sink):
    def __init__(self, path: Path | str):
        self.path = Path(path)

    async def write(self, data, *, request=None):
        if not isinstance(data, list):
            data = [data]
        for item in data:
            if "id" in item:
                name = f"{item['id']}.json"
            else:
                name = md5(json.dumps(item).encode())

            async with aiofiles.open(self.path / name, mode=mode, encoding="utf8"):
                await fd.write(item)
        return data


class CSVSink(Sink):
    """Write list[dict] as CSV. Header written on first write."""
    def __init__(self, path: Path | str, *, fieldnames: list[str] | None = None):
        self.path = Path(path)
        self.fieldnames = fieldnames
        self._header_written = False

    async def write(self, data, *, request=None):
        if not isinstance(data, list):
            data = [data]
        if not data:
            return data
        fieldnames = self.fieldnames or list(data[0].keys())

        def _do():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not self._header_written:
                    writer.writeheader()
                    self._header_written = True
                writer.writerows(data)
        await asyncio.to_thread(_do)
        return data


class ParquetSink(Sink):
    """Write list[dict] as Parquet via polars (optional dependency)."""
    def __init__(self, path: Path | str, *, append: bool = False):
        self.path = Path(path)
        self.append = append

    async def write(self, data, *, request=None):
        try:
            import polars as pl
        except ImportError as e:
            raise RuntimeError("ParquetSink requires `polars`") from e

        def _do():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            df = pl.DataFrame(data)
            if self.append and self.path.exists():
                df = pl.concat([pl.read_parquet(self.path), df])
            df.write_parquet(self.path)
        await asyncio.to_thread(_do)
        return data

class LakeSink(Sink):
    """Single write path to the consolidated warehouse.

    The sink is deliberately dumb: it writes one batch, closes the previous
    version of each row, and returns. Schema enforcement, backups, and
    query are the *storage layer's* job, not this class's.
    """

    def __init__(
        self,
        engine,                       # SQLAlchemy Engine (Postgres/DuckDB)
        table: str,
        primary_key: Sequence[str],
        *,
        columns: Sequence[str],
        batch_size: int = 5000,
    ):
        self.engine = engine
        self.table = table
        self.primary_key = list(primary_key)
        self.columns = list(columns)
        self.batch_size = batch_size

    async def write(self, data, *, request=None):
        if not isinstance(data, list):
            data = [data]
        if not data:
            return data
        # Row-level history via upsert; the DB owns the versioning
        await asyncio.to_thread(self._write_sync, data)
        return data

    def _write_sync(self, data):
        import sqlalchemy as sa
        meta = sa.MetaData()
        # Table is reflected at startup elsewhere; here we just merge
        tbl = sa.Table(self.table, meta, autoload_with=self.engine)
        with self.engine.begin() as conn:
            for start in range(0, len(data), self.batch_size):
                chunk = data[start : start + self.batch_size]
                self._close_previous_versions(conn, tbl, chunk)
                conn.execute(tbl.insert(), chunk)

    def _close_previous_versions(self, conn, tbl, chunk):
        import sqlalchemy as sa
        from datetime import datetime, timezone
        for row in chunk:
            pred = sa.and_(
                *[tbl.c[k] == row[k] for k in self.primary_key],
                tbl.c["valid_to"].is_(None),
            )
            conn.execute(
                tbl.update()
                .where(pred)
                .values(valid_to=datetime.now(timezone.utc))
            )


class MultiSink(Sink):
    """Fan out one write to several sinks in parallel."""
    def __init__(self, *sinks: Sink):
        self.sinks = sinks

    async def write(self, data, *, request=None):
        await asyncio.gather(*(s.write(data, request=request) for s in self.sinks))
        return data


async def pipe_sink(data: Any, dependencies, sink=None, **kwargs) -> Any:
    """Default `pipe` stage handler. Returns data unchanged after writing."""
    if sink is None:
        return data
    return await sink.write(data)
