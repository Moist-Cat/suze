"""Progress observer fired from inside `DataSource.fetch`.

The observer is a contextvar so any `fetch` call (including recursive
dependency resolution) reports its own state without the caller having
to poll. This is what makes short-circuit visible: a cache hit fires
`cache_hit`, not nothing.
"""

from contextvars import ContextVar
from typing import Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class Observer(Protocol):
    async def before_node(self, request): ...
    def cache_hit(self, request, value): ...
    def node_started(self, request): ...
    def node_completed(self, request, result): ...
    def node_failed(self, request, exc): ...
    def node_deduplicated(self, request, existing_task): ...


class NullObserver:
    async def before_node(self, request): pass
    def cache_hit(self, request, value): pass
    def node_started(self, request): pass
    def node_completed(self, request, result): pass
    def node_failed(self, request, exc): pass
    def node_deduplicated(self, request, existing_task): pass

class LineageObserver(Observer):
    """One LineageObserver per pipeline run. Attach alongside PipelineObserver."""

    def __init__(self, pipeline_id: str, db_engine=None, jsonl_path: Optional[Path] = None):
        self.pipeline_id = pipeline_id
        self.run_id = str(uuid.uuid4())
        self.started_at = datetime.now(timezone.utc)
        self.db = db_engine
        self.jsonl = jsonl_path
        self._seen_edges: set[tuple[str, str]] = set()

    async def before_node(self, request):
        # emit edges once (we see the request, which carries its deps)
        for dep in request.dependency_requirements:
            edge = (dep.request_id, request.request_id)
            if edge in self._seen_edges:
                continue
            self._seen_edges.add(edge)
            self._emit({
                "eventType": "COMPLETE",
                "eventTime": datetime.now(timezone.utc).isoformat(),
                "run": {"runId": self.run_id, "jobNamespace": "suze",
                        "jobName": self.pipeline_id},
                "inputs":  [{"namespace": dep.source.source_id if dep.source else "?",
                             "name": dep.request_id}],
                "outputs": [{"namespace": request.source.source_id if request.source else "?",
                             "name": request.request_id}],
            })
        if self.jsonl:
            self._emit({
                "eventType": "START",
                "eventTime": datetime.now(timezone.utc).isoformat(),
                "run": {"runId": self.run_id, "jobNamespace": "suze",
                        "jobName": self.pipeline_id},
            })

    def cache_hit(self, request, value):    pass
    def node_started(self, request):        pass
    def node_completed(self, request, result): pass
    def node_failed(self, request, exc):    pass
    def node_deduplicated(self, request, existing_task): pass

    def _emit(self, event: dict):
        if self.jsonl:
            self.jsonl.parent.mkdir(parents=True, exist_ok=True)
            with self.jsonl.open("a") as f:
                f.write(json.dumps(event) + "\n")
        if self.db is not None:
            # Fire-and-forget; a broken lineage DB must not fail a pipeline.
            try:
                self._persist(event)
            except Exception:
                pass


_observer: ContextVar[Optional[Observer]] = ContextVar("suze_observer", default=None)


def get_observer() -> Optional[Observer]:
    return _observer.get()


def set_observer(obs: Observer):
    return _observer.set(obs)


def reset_observer(token):
    _observer.reset(token)
