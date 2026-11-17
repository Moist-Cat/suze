"""Async pipeline manager and state observer.

One event loop owns the manager. Pipelines are `asyncio.Task`s. Progress
comes from `PipelineObserver`, which is bound via contextvar and fires
from inside `DataSource.fetch` — no monitor thread, no lock polling,
no state inference.
"""

import asyncio
import time
import traceback
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from suze.observe import Observer, set_observer, reset_observer
from suze.request import DataRequest
from suze.log import get_logger

logger = get_logger("pipeline")


class NodeStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class PipelineStatus(Enum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class NodeState:
    request_id: str
    source_id: str
    status: NodeStatus = NodeStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        # `result` can be huge; keep it out of the status dict.
        safe_meta = {k: v for k, v in self.metadata.items() if k != "result"}
        return {
            "request_id": self.request_id,
            "source_id": self.source_id,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": (
                (self.end_time - self.start_time).total_seconds()
                if self.start_time and self.end_time
                else None
            ),
            "error": self.error,
            "dependencies": self.dependencies,
            "dependents": self.dependents,
            "last_updated": self.last_updated.isoformat(),
            "metadata": safe_meta,
        }


@dataclass
class PipelineState:
    pipeline_id: str
    tail_request_id: str
    status: PipelineStatus = PipelineStatus.CREATED
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_nodes: int = 0
    completed_nodes: int = 0
    failed_nodes: int = 0
    cancelled_nodes: int = 0
    execution_order: List[List[str]] = field(default_factory=list)
    node_states: Dict[str, NodeState] = field(default_factory=dict)
    result: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        completed_or_skipped = sum(
            1 for ns in self.node_states.values()
            if ns.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
        )
        return {
            "pipeline_id": self.pipeline_id,
            "tail_request_id": self.tail_request_id,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "duration": (
                (self.completed_at - self.started_at).total_seconds()
                if self.started_at and self.completed_at
                else None
            ),
            "progress": {
                "total": self.total_nodes,
                "completed": completed_or_skipped,
                "failed": self.failed_nodes,
                "cancelled": self.cancelled_nodes,
                "percentage": (
                    completed_or_skipped / self.total_nodes * 100
                    if self.total_nodes else 0
                ),
            },
            "error": self.error,
            "node_summary": {
                nid: ns.to_dict() for nid, ns in self.node_states.items()
            },
            "execution_order": self.execution_order,
            "metadata": self.metadata,
        }


class PipelineObserver(Observer):
    """Receives `fetch` events and mutates `PipelineState`. Single writer."""

    def __init__(self, manager: "PipelineManager", pipeline_id: str):
        self.manager = manager
        self.pipeline_id = pipeline_id
        self.state = manager.pipelines[pipeline_id]
        self.pause_event = asyncio.Event()
        self.pause_event.set()  # starts unpaused
        self.cancel_requested = False

    # --- pause / cancel checkpoint ---
    async def before_node(self, request):
        await self.pause_event.wait()
        if self.cancel_requested:
            raise asyncio.CancelledError(
                f"Pipeline {self.pipeline_id} cancelled"
            )

    # --- cache hit (short-circuit made visible) ---
    def cache_hit(self, request, value):
        self._mark_completed(request, value, cached=True)

    def node_started(self, request):
        node = self.state.node_states.get(request.request_id)
        if node is None:
            return
        if node.status in (
            NodeStatus.COMPLETED,
            NodeStatus.SKIPPED,
            NodeStatus.FAILED,
        ):
            return
        node.status = NodeStatus.RUNNING
        node.start_time = datetime.now()
        node.last_updated = datetime.now()
        self.manager._notify(self.pipeline_id)

    def node_completed(self, request, result):
        self._mark_completed(request, result, cached=False)

    def node_failed(self, request, exc):
        node = self.state.node_states.get(request.request_id)
        if node is None:
            return
        if node.status in (NodeStatus.FAILED, NodeStatus.CANCELLED):
            return
        node.end_time = datetime.now()
        node.last_updated = datetime.now()
        node.error = str(exc) or type(exc).__name__
        if isinstance(exc, asyncio.CancelledError):
            node.status = NodeStatus.CANCELLED
            self.state.cancelled_nodes += 1
        else:
            node.status = NodeStatus.FAILED
            node.metadata["error_details"] = {
                "traceback": "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                ),
                "exception_type": type(exc).__name__,
                "timestamp": datetime.now().isoformat(),
            }
            self.state.failed_nodes += 1
        self.manager._notify(self.pipeline_id)

    def node_deduplicated(self, request, existing_task):
        node = self.state.node_states.get(request.request_id)
        if node is None:
            return
        node.metadata["deduplicated"] = True
        node.last_updated = datetime.now()
        self.manager._notify(self.pipeline_id)

    def _mark_completed(self, request, result, cached: bool):
        node = self.state.node_states.get(request.request_id)
        if node is None:
            return
        already_counted = node.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
        node.status = NodeStatus.COMPLETED
        node.end_time = datetime.now()
        node.last_updated = datetime.now()
        node.metadata["result"] = result
        if cached:
            node.metadata["cached"] = True
        if not already_counted:
            self.state.completed_nodes += 1
        self.manager._notify(self.pipeline_id)


class PipelineManager:
    def __init__(self):
        self.pipelines: Dict[str, PipelineState] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._observers: Dict[str, PipelineObserver] = {}
        self._status_listeners: List[Callable] = []
        self.request_map: Dict[str, DataRequest] = {}
        logger.info("PipelineManager initialized")

    # ---------- registration ----------

    def register_pipeline(
        self, tail_request: DataRequest, name: Optional[str] = None
    ) -> str:
        pipeline_id = name or tail_request.request_id
        node_states, execution_order = self._build_dag(tail_request)
        state = PipelineState(
            pipeline_id=pipeline_id,
            tail_request_id=tail_request.request_id,
            total_nodes=len(node_states),
            execution_order=execution_order,
            metadata={
                "request_id": tail_request.request_id,
                "stages": list(tail_request.stages.keys()),
                "cache_key": hash(tail_request),
            },
        )
        state.node_states = node_states
        self.pipelines[pipeline_id] = state

        self._analyze_cache(pipeline_id, tail_request)
        state.completed_nodes = sum(
            1 for ns in node_states.values()
            if ns.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
        )

        logger.info(
            f"Pipeline {pipeline_id} registered: {len(node_states)} nodes, "
            f"{state.completed_nodes} already cached"
        )
        self._notify(pipeline_id)
        return pipeline_id

    def _build_dag(
        self, request: DataRequest
    ) -> Tuple[Dict[str, NodeState], List[List[str]]]:
        node_states: Dict[str, NodeState] = {}
        adjacency: Dict[str, List[str]] = defaultdict(list)
        reverse: Dict[str, List[str]] = defaultdict(list)

        queue = deque([request])
        visited = set()
        while queue:
            current = queue.popleft()
            if current.request_id in visited:
                continue
            visited.add(current.request_id)
            current.validate()

            self.request_map[current.request_id] = current
            node_states[current.request_id] = NodeState(
                request_id=current.request_id,
                source_id=current.source.source_id if current.source else "unknown",
                metadata={
                    "stages": list(current.stages),
                    "cache_key": hash(current),
                },
            )

            for dep in current.dependency_requirements:
                adjacency[dep.request_id].append(current.request_id)
                reverse[current.request_id].append(dep.request_id)
                node_states[current.request_id].dependencies.append(
                    dep.request_id
                )
                if dep.request_id not in visited:
                    queue.append(dep)
                if dep.request_id not in node_states:
                    node_states[dep.request_id] = NodeState(
                        request_id=dep.request_id,
                        source_id=(
                            dep.source.source_id if dep.source else "unknown"
                        ),
                        metadata={
                            "stages": list(dep.stages),
                            "cache_key": hash(dep),
                        },
                    )

        for nid, deps in reverse.items():
            for d in deps:
                node_states[d].dependents.append(nid)

        order = self._calculate_execution_levels(adjacency, node_states.keys())
        return node_states, order

    def _analyze_cache(
        self,
        pipeline_id: str,
        request: DataRequest,
        visited: Optional[set] = None,
    ) -> None:
        if visited is None:
            visited = set()
        if request.request_id in visited:
            return
        visited.add(request.request_id)

        node = self.pipelines[pipeline_id].node_states.get(request.request_id)
        if node is None or node.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED):
            return

        if request.source and request.source.cache.exists(hash(request)):
            node.status = NodeStatus.COMPLETED
            node.metadata["cached"] = True
            # Descendants will never be fetched — mark SKIPPED
            stack = list(request.dependency_requirements)
            while stack:
                dep = stack.pop()
                dep_node = self.pipelines[pipeline_id].node_states.get(
                    dep.request_id
                )
                if dep_node is None or dep_node.status == NodeStatus.SKIPPED:
                    continue
                dep_node.status = NodeStatus.SKIPPED
                stack.extend(dep.dependency_requirements)
        else:
            for dep in request.dependency_requirements:
                self._analyze_cache(pipeline_id, dep, visited)

    def _calculate_execution_levels(
        self, adjacency: Dict[str, List[str]], node_ids
    ) -> List[List[str]]:
        in_degree = {nid: 0 for nid in node_ids}
        for _, neighbors in adjacency.items():
            for n in neighbors:
                in_degree[n] = in_degree.get(n, 0) + 1
        levels = []
        current = [nid for nid in node_ids if in_degree[nid] == 0]
        while current:
            levels.append(current.copy())
            nxt = []
            for nid in current:
                for n in adjacency.get(nid, []):
                    in_degree[n] -= 1
                    if in_degree[n] == 0:
                        nxt.append(n)
            current = nxt
        return levels

    # ---------- execution ----------

    def start_pipeline(self, pipeline_id: str) -> asyncio.Task:
        """Schedule the pipeline. Returns the Task (caller can await or not)."""
        if pipeline_id not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_id} not found")
        existing = self._tasks.get(pipeline_id)
        if existing is not None and not existing.done():
            raise RuntimeError(f"Pipeline {pipeline_id} already running")

        state = self.pipelines[pipeline_id]
        state.status = PipelineStatus.RUNNING
        state.started_at = datetime.now()
        state.completed_at = None
        state.error = None
        # Reset runtime counters; cached/completed state is preserved
        state.completed_nodes = sum(
            1 for ns in state.node_states.values()
            if ns.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
        )
        state.failed_nodes = 0
        state.cancelled_nodes = 0

        observer = PipelineObserver(self, pipeline_id)
        self._observers[pipeline_id] = observer
        task = asyncio.create_task(self._run(pipeline_id, observer))
        self._tasks[pipeline_id] = task
        self._notify(pipeline_id)
        return task

    async def run_pipeline(self, pipeline_id: str) -> Any:
        return await self.start_pipeline(pipeline_id)

    async def wait(self, pipeline_id: str, timeout: Optional[float] = None) -> Any:
        task = self._tasks.get(pipeline_id)
        if task is None:
            raise ValueError(f"Pipeline {pipeline_id} not started")
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout)

    async def _run(self, pipeline_id: str, observer: PipelineObserver):
        state = self.pipelines[pipeline_id]
        tail = self.request_map[state.tail_request_id]
        token = set_observer(observer)
        try:
            result = await tail.fetch()
            state.result = result
            state.status = PipelineStatus.COMPLETED
        except asyncio.CancelledError:
            state.status = PipelineStatus.CANCELLED
            raise
        except Exception as e:
            state.status = PipelineStatus.FAILED
            state.error = str(e)
            logger.error(f"Pipeline {pipeline_id} failed: {e}")
            logger.error(traceback.format_exc())
        finally:
            reset_observer(token)
            state.completed_at = datetime.now()
            self._finalize(pipeline_id)
            self._notify(pipeline_id)
            self._observers.pop(pipeline_id, None)

    def _finalize(self, pipeline_id: str):
        state = self.pipelines[pipeline_id]
        if state.status == PipelineStatus.FAILED:
            self._mark_failed_descendants(pipeline_id)
        logger.info(
            f"Pipeline {pipeline_id} finalized: {state.status.value} "
            f"({state.completed_nodes} ok / {state.failed_nodes} failed / "
            f"{state.cancelled_nodes} cancelled / "
            f"{self._count_skipped(pipeline_id)} skipped)"
        )

    def _mark_failed_descendants(self, pipeline_id: str):
        state = self.pipelines[pipeline_id]
        failed = {
            nid for nid, ns in state.node_states.items()
            if ns.status == NodeStatus.FAILED
        }
        if not failed:
            return
        to_skip = set()
        for f in failed:
            queue = deque(state.node_states[f].dependents)
            while queue:
                d = queue.popleft()
                if d not in to_skip:
                    to_skip.add(d)
                    queue.extend(state.node_states[d].dependents)
        for nid in to_skip:
            ns = state.node_states[nid]
            if ns.status == NodeStatus.PENDING:
                ns.status = NodeStatus.SKIPPED
                ns.error = f"Dependency failed: {failed}"
                ns.last_updated = datetime.now()

    def _count_skipped(self, pipeline_id: str) -> int:
        return sum(
            1 for ns in self.pipelines[pipeline_id].node_states.values()
            if ns.status == NodeStatus.SKIPPED
        )

    # ---------- control ----------

    def pause_pipeline(self, pipeline_id: str):
        obs = self._observers.get(pipeline_id)
        if obs is None:
            raise ValueError(f"Pipeline {pipeline_id} not running")
        obs.pause_event.clear()
        self.pipelines[pipeline_id].status = PipelineStatus.PAUSED
        self._notify(pipeline_id)

    def resume_pipeline(self, pipeline_id: str):
        obs = self._observers.get(pipeline_id)
        if obs is None:
            raise ValueError(f"Pipeline {pipeline_id} not running")
        obs.pause_event.set()
        self.pipelines[pipeline_id].status = PipelineStatus.RUNNING
        self._notify(pipeline_id)

    async def cancel_pipeline(self, pipeline_id: str, reason: str = "Cancelled"):
        task = self._tasks.get(pipeline_id)
        state = self.pipelines.get(pipeline_id)
        if task is None or state is None:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        for ns in state.node_states.values():
            if ns.status == NodeStatus.PENDING:
                ns.status = NodeStatus.CANCELLED
                ns.error = reason
                ns.end_time = datetime.now()
                state.cancelled_nodes += 1
        state.status = PipelineStatus.CANCELLED
        state.error = reason
        self._notify(pipeline_id)

    stop_pipeline = cancel_pipeline

    # ---------- introspection ----------

    def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        if pipeline_id not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_id} not found")
        return self.pipelines[pipeline_id].to_dict()

    def get_all_pipelines_status(self) -> Dict[str, Any]:
        return {
            "total": len(self.pipelines),
            "pipelines": {pid: s.to_dict() for pid, s in self.pipelines.items()},
        }

    def get_node_details(self, pipeline_id: str, node_id: str) -> Dict[str, Any]:
        state = self.pipelines.get(pipeline_id)
        if state is None:
            raise ValueError(f"Pipeline {pipeline_id} not found")
        ns = state.node_states.get(node_id)
        if ns is None:
            raise ValueError(f"Node {node_id} not found")
        d = ns.to_dict()
        d["pipeline_id"] = pipeline_id
        d["pipeline_status"] = state.status.value
        return d

    def get_pipeline_visualization_data(self, pipeline_id: str) -> Dict[str, Any]:
        state = self.pipelines.get(pipeline_id)
        if state is None:
            raise ValueError(f"Pipeline {pipeline_id} not found")
        nodes = [
            {
                "id": nid,
                "label": f"{ns.source_id}\n{nid[:8]}",
                "status": ns.status.value,
                "source_id": ns.source_id,
            }
            for nid, ns in state.node_states.items()
        ]
        edges = [
            {"from": dep, "to": nid}
            for nid, ns in state.node_states.items()
            for dep in ns.dependencies
        ]
        timeline = [
            {
                "batch": i + 1,
                "nodes": batch,
                "status": self._batch_status(state, batch),
            }
            for i, batch in enumerate(state.execution_order)
        ]
        return {
            "pipeline_id": pipeline_id,
            "status": state.status.value,
            "nodes": nodes,
            "edges": edges,
            "timeline": timeline,
        }

    def _batch_status(self, state: PipelineState, batch: List[str]) -> str:
        statuses = [state.node_states[n].status for n in batch]
        if any(s == NodeStatus.FAILED for s in statuses):
            return "failed"
        if all(s in (NodeStatus.COMPLETED, NodeStatus.SKIPPED) for s in statuses):
            return "completed"
        if any(s == NodeStatus.RUNNING for s in statuses):
            return "running"
        return "pending"

    def register_status_listener(self, callback: Callable):
        self._status_listeners.append(callback)

    def _notify(self, pipeline_id: str):
        state = self.pipelines.get(pipeline_id)
        if state is None:
            return
        snapshot = state.to_dict()
        for cb in self._status_listeners:
            try:
                cb(pipeline_id, snapshot)
            except Exception as e:
                logger.error(f"Status listener failed: {e}")


def sync_run(request: DataRequest) -> Any:
    """Blocking convenience for scripts and REPLs."""

    async def _run():
        manager = PipelineManager()
        pid = manager.register_pipeline(request)
        await manager.run_pipeline(pid)
        state = manager.pipelines[pid]
        if state.status != PipelineStatus.COMPLETED:
            raise RuntimeError(
                f"Pipeline {pid} ended with {state.status.value}: {state.error}"
            )
        return state.result

    return asyncio.run(_run())
