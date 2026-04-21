import asyncio
import threading
import time
from enum import Enum
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import uuid
from collections import deque, defaultdict
from abc import ABC, abstractmethod
import traceback
from contextlib import contextmanager

from suze.request import DataRequest
from suze.source import DataSource
from suze.log import logged

# ==================== Enums and Data Classes ====================


class NodeStatus(Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"
    SKIPPED = "skipped"


class PipelineStatus(Enum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    WAITING = "waiting"


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
            "metadata": self.metadata,
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
    current_batch: int = 0
    total_batches: int = 0
    error: Optional[str] = None
    node_states: Dict[str, NodeState] = field(default_factory=dict)
    execution_order: List[List[str]] = field(default_factory=list)
    control_flags: Dict[str, bool] = field(default_factory=dict)
    result: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
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
                "completed": self.completed_nodes,
                "failed": self.failed_nodes,
                "cancelled": self.cancelled_nodes,
                "percentage": (
                    (self.completed_nodes / self.total_nodes * 100)
                    if self.total_nodes > 0
                    else 0
                ),
            },
            "current_batch": self.current_batch,
            "total_batches": self.total_batches,
            "error": self.error,
            "node_summary": {
                node_id: state.to_dict() for node_id, state in self.node_states.items()
            },
            "execution_order": self.execution_order,
            "control_flags": self.control_flags,
            "result": self.result,
            "metadata": self.metadata,
        }


# ==================== Pipeline Manager ====================


@logged
class PipelineManager:
    """
    Manages execution of ETL pipelines with DAG structure.
    Uses DFS execution (via recursive fetch) for performance with cache short‑circuiting.
    Observability is achieved by monitoring request locks in a background thread.
    """

    def __init__(self):
        self.logger.info("Initializing PipelineManager")
        self.pipelines: Dict[str, PipelineState] = {}
        self._pipeline_threads: Dict[str, threading.Thread] = {}
        self._monitor_threads: Dict[str, threading.Thread] = {}
        self._node_threads: Dict[str, Dict[str, threading.Thread]] = defaultdict(dict)

        # Control synchronization
        self._pipeline_events: Dict[str, threading.Event] = {}
        self._pipeline_pause_events: Dict[str, threading.Event] = {}
        self._pipeline_stop_flags: Dict[str, bool] = {}
        self._pipeline_locks: Dict[str, threading.Lock] = {}

        # Observability
        self._status_listeners: List[callable] = []
        self._metric_collectors: List[callable] = []

        self.request_map: Dict[str, "DataRequest"] = {}

        # Background threads control
        self._running = True
        self._manager_lock = threading.Lock()  # protects shared structures
        self.logger.debug("PipelineManager initialized successfully")

    def start(self):
        self.logger.info("Starting PipelineManager")
        self._running = True
        self.logger.debug("PipelineManager started")

    def stop(self):
        self.logger.info("Stopping PipelineManager")
        self._running = False

        # Stop all running pipelines
        for pipeline_id in list(self.pipelines.keys()):
            self.stop_pipeline(pipeline_id)

        # Wait for all threads to finish
        for pipeline_id, thread in self._pipeline_threads.items():
            if thread.is_alive():
                self.logger.info(f"Waiting for pipeline thread: {pipeline_id}")
                thread.join(timeout=5.0)

        for pipeline_id, thread in self._monitor_threads.items():
            if thread.is_alive():
                thread.join(timeout=1.0)

        self.logger.info("PipelineManager stopped successfully")

    def register_pipeline(self, tail_request: "DataRequest") -> str:
        pipeline_id = tail_request.request_id
        self.logger.info(f"Registering new pipeline with ID: {pipeline_id}")
        self.logger.debug(
            f"Tail request details: source_id={tail_request.source.source_id}, "
            f"stages={list(tail_request.stages.keys())}"
        )

        # Build DAG structure from the tail request
        self.logger.debug("Building DAG from tail request")
        node_states, execution_order = self._build_dag(tail_request)

        # Initialize pipeline state
        pipeline_state = PipelineState(
            pipeline_id=pipeline_id,
            tail_request_id=tail_request.request_id,
            total_nodes=len(node_states),
            execution_order=execution_order,
            total_batches=len(execution_order),
            metadata={
                "request_id": tail_request.request_id,
                "stages": list(tail_request.stages.keys()),
                "cache_key": hash(tail_request),
            },
            result=None,
        )
        pipeline_state.node_states = node_states

        # Initialize control structures
        with self._manager_lock:
            self.pipelines[pipeline_id] = pipeline_state
            self._pipeline_events[pipeline_id] = threading.Event()
            self._pipeline_pause_events[pipeline_id] = threading.Event()
            self._pipeline_pause_events[pipeline_id].set()  # Initially not paused
            self._pipeline_stop_flags[pipeline_id] = False
            self._pipeline_locks[pipeline_id] = threading.Lock()

        self._notify_status_change(pipeline_id)

        self.logger.info(f"Pipeline {pipeline_id} registered successfully")

        # Run cache analysis and mark already completed nodes
        self._analyze_cache(pipeline_id, tail_request)

        # Update completed_nodes count based on cached nodes
        with self._manager_lock:
            pipeline_state.completed_nodes = sum(
                1
                for ns in pipeline_state.node_states.values()
                if (
                    ns.status == NodeStatus.COMPLETED or ns.status == NodeStatus.SKIPPED
                )
            )

        self.logger.info(
            f"DAG built with {len(node_states)} nodes, "
            f"{pipeline_state.completed_nodes} already cached"
        )

        return pipeline_id

    def _build_dag(
        self, request: "DataRequest"
    ) -> Tuple[Dict[str, NodeState], List[List[str]]]:
        """Build the DAG structure without cache analysis (graph only)."""
        self.logger.debug(f"Building DAG for request: {request.request_id}")
        node_states: Dict[str, NodeState] = {}
        adjacency: Dict[str, List[str]] = defaultdict(list)
        reverse_adjacency: Dict[str, List[str]] = defaultdict(list)

        queue = deque([request])
        visited = set()

        while queue:
            current = queue.popleft()
            if current.request_id in visited:
                continue

            visited.add(current.request_id)
            self.logger.debug(
                f"Processing node: {current.request_id}, source: {current.source.source_id}"
            )

            with self._manager_lock:
                if current.request_id not in self.request_map:
                    self.request_map[current.request_id] = current
                    self.logger.debug(f"Added to request_map: {current.request_id}")

            # Expire cache (as before)
            current.source.cache.expire(hash(current))

            # Create node state
            node_state = NodeState(
                request_id=current.request_id,
                source_id=current.source.source_id if current.source else "unknown",
                dependencies=[],
                dependents=[],
                metadata={
                    "stages": list(current.stages),
                    "cache_key": hash(current),
                },
            )
            node_states[current.request_id] = node_state

            for dep_request in current.dependency_requirements:
                self.logger.debug(
                    f"Processing dependency: {dep_request.request_id} -> {current.request_id}"
                )

                if dep_request.request_id not in visited:
                    queue.append(dep_request)

                adjacency[dep_request.request_id].append(current.request_id)
                reverse_adjacency[current.request_id].append(dep_request.request_id)

                node_state.dependencies.append(dep_request.request_id)

                if dep_request.request_id not in node_states:
                    dep_state = NodeState(
                        request_id=dep_request.request_id,
                        source_id=(
                            dep_request.source.source_id
                            if dep_request.source
                            else "unknown"
                        ),
                        dependencies=[],
                        dependents=[],
                        metadata={
                            "stages": dep_request.stages,
                            "cache_key": dep_request.cache_key,
                        },
                    )
                    node_states[dep_request.request_id] = dep_state

        # Build dependents lists
        for node_id, deps in reverse_adjacency.items():
            for dep_id in deps:
                node_states[dep_id].dependents.append(node_id)

        # Calculate topological levels (for UI)
        execution_order = self._calculate_execution_levels(
            adjacency, node_states.keys()
        )

        return node_states, execution_order

    def _analyze_cache(
        self,
        pipeline_id: str,
        request: "DataRequest",
        visited: Optional[Set[str]] = None,
    ):
        """
        Recursively mark nodes as completed if their cache key exists.
        A cached node implies its entire subtree is also cached, so we stop traversal.
        """
        if visited is None:
            visited = set()

        if request.request_id in visited:
            return
        visited.add(request.request_id)

        pipeline_state = self.pipelines[pipeline_id]
        node_state = pipeline_state.node_states[request.request_id]
        key = hash(request)

        if (node_state.status == NodeStatus.SKIPPED) or (
            node_state.status == NodeStatus.COMPLETED
        ):  # visited
            return

        if request.source.cache.exists(key):
            self.logger.debug(
                f"Cache hit for {request.request_id}, marking as completed"
            )
            node_state.status = NodeStatus.COMPLETED
            node_state.metadata["cached"] = True

            queue = []
            for dep in request.dependency_requirements:
                queue.append(dep)

            while queue:
                req = queue.pop()
                self.logger.debug(
                    f"Short-circuit {req.request_id} from {request.request_id}, marking as skipped"
                )

                # It doesn't matter if one of these is cached
                node_state = pipeline_state.node_states[req.request_id]
                node_state.status = NodeStatus.SKIPPED

                for dep in req.dependency_requirements:
                    queue.append(dep)
        else:
            self.logger.debug(
                f"Cache miss for {request.request_id}, traversing dependencies"
            )
            for dep in request.dependency_requirements:
                self._analyze_cache(pipeline_id, dep, visited)

    def _calculate_execution_levels(
        self, adjacency: Dict[str, List[str]], node_ids: Set[str]
    ) -> List[List[str]]:
        """Same as before – for UI only."""
        self.logger.debug(f"Calculating execution levels for {len(node_ids)} nodes")
        in_degree = {node_id: 0 for node_id in node_ids}
        for node_id, neighbors in adjacency.items():
            for neighbor in neighbors:
                in_degree[neighbor] = in_degree.get(neighbor, 0) + 1

        levels = []
        current_level = [node_id for node_id in node_ids if in_degree[node_id] == 0]

        level_index = 0
        while current_level:
            levels.append(current_level.copy())
            next_level = []
            for node_id in current_level:
                for neighbor in adjacency.get(node_id, []):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_level.append(neighbor)
            current_level = next_level
            level_index += 1

        return levels

    def run_pipeline(self, pipeline_id: str) -> str:
        self.logger.info(f"Attempting to run pipeline: {pipeline_id}")

        with self._manager_lock:
            if pipeline_id not in self.pipelines:
                self.logger.error(f"Pipeline {pipeline_id} not found")
                raise ValueError(f"Pipeline {pipeline_id} not found")

            # Check if already running
            if (
                pipeline_id in self._pipeline_threads
                and self._pipeline_threads[pipeline_id].is_alive()
            ):
                self.logger.warning(f"Pipeline {pipeline_id} is already running")
                raise RuntimeError(f"Pipeline {pipeline_id} is already running")

            self._pipeline_stop_flags[pipeline_id] = False

            # Create execution thread
            thread = threading.Thread(
                target=self._execute_pipeline,
                args=(pipeline_id,),
                name=f"Pipeline-{pipeline_id}",
                daemon=True,
            )
            self._pipeline_threads[pipeline_id] = thread

            # Update pipeline state
            pipeline_state = self.pipelines[pipeline_id]
            pipeline_state.status = PipelineStatus.RUNNING
            pipeline_state.started_at = datetime.now()
            pipeline_state.error = None

        self._notify_status_change(pipeline_id)
        thread.start()
        self.logger.info(f"Pipeline {pipeline_id} execution started")
        return pipeline_id

    def _execute_pipeline(self, pipeline_id: str):
        pipeline_state = self.pipelines[pipeline_id]
        tail_request = self.request_map[pipeline_state.tail_request_id]

        # Start the monitor thread
        monitor_thread = threading.Thread(
            target=self._monitor_pipeline,
            args=(pipeline_id,),
            name=f"Monitor-{pipeline_id}",
            daemon=True,
        )
        with self._manager_lock:
            self._monitor_threads[pipeline_id] = monitor_thread
        monitor_thread.start()

        try:
            self.logger.info(
                f"Starting DFS execution from tail node: {tail_request.request_id}"
            )
            result = self._execute_node(pipeline_id, tail_request.request_id)
            pipeline_state.result = result
            self.logger.info(f"Tail node completed successfully")
        except Exception as e:
            self.logger.error(f"Pipeline {pipeline_id} execution failed: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            pipeline_state.status = PipelineStatus.FAILED
            pipeline_state.error = str(e)
        finally:
            # Stop monitor thread (by setting flag and waiting)
            self._pipeline_stop_flags[pipeline_id] = (
                True  # monitor will exit on next iteration
            )
            monitor_thread.join(timeout=2.0)
            with self._manager_lock:
                if pipeline_id in self._monitor_threads:
                    del self._monitor_threads[pipeline_id]
            self._finalize_pipeline(pipeline_id)

    def _execute_node(self, pipeline_id: str, node_id: str) -> Any:
        """
        Execute a single node (called only for the tail; dependencies are handled by fetch).
        This wrapper checks control flags, updates state, and notifies listeners.
        """
        pipeline_state = self.pipelines[pipeline_id]
        node_state = pipeline_state.node_states[node_id]
        request = self.request_map[node_id]

        # If already cached, return result from metadata (if stored)
        if node_state.status == NodeStatus.COMPLETED and node_state.metadata.get(
            "cached"
        ):
            self.logger.debug(f"Node {node_id} already cached, returning immediately")
            return node_state.metadata.get("result")

        # Wait if paused
        pause_event = self._pipeline_pause_events[pipeline_id]
        while not pause_event.is_set():
            time.sleep(0.1)  # simple polling

        if self._pipeline_stop_flags[pipeline_id]:
            node_state.status = NodeStatus.CANCELLED
            raise RuntimeError("Pipeline stopped")  # or custom exception

        # Update state to RUNNING
        node_state.status = NodeStatus.RUNNING
        node_state.start_time = datetime.now()
        node_state.last_updated = datetime.now()
        self._notify_status_change(pipeline_id)

        try:
            result = asyncio.run(request.fetch())  # no await

            # Success
            node_state.status = NodeStatus.COMPLETED
            node_state.end_time = datetime.now()
            node_state.metadata["result"] = result
            with self._manager_lock:
                pipeline_state.completed_nodes += 1

            self.logger.info(f"Node {node_id} completed successfully")
            self._notify_status_change(pipeline_id)
            return result
        except Exception as e:
            # Failure
            node_state.status = NodeStatus.FAILED
            node_state.error = str(e)
            node_state.end_time = datetime.now()
            node_state.metadata["error_details"] = {
                "traceback": traceback.format_exc(),
                "exception_type": type(e).__name__,
                "timestamp": datetime.now().isoformat(),
            }
            with self._manager_lock:
                pipeline_state.failed_nodes += 1
            self.logger.error(f"Node {node_id} failed: {e}")
            self._notify_status_change(pipeline_id)
            raise

    def _monitor_pipeline(self, pipeline_id: str):
        """
        Background thread that watches request locks to update node states.
        Polls every 0.1 second.
        """
        pipeline_state = self.pipelines[pipeline_id]
        self.logger.debug(f"Starting monitor for pipeline {pipeline_id}")

        while self._running and not self._pipeline_stop_flags.get(pipeline_id, False):
            time.sleep(0.1)  # poll interval

            for node_id, node_state in list(pipeline_state.node_states.items()):
                request = self.request_map.get(node_id)
                if not request:
                    continue

                # Skip nodes already finalized
                if node_state.status in (
                    NodeStatus.COMPLETED,
                    NodeStatus.FAILED,
                    NodeStatus.CANCELLED,
                    NodeStatus.SKIPPED,
                ):
                    continue

                # In threading, we need a way to check if the node's lock is held.
                # This assumes request has a threading.Lock attribute.
                lock_held = request.lock.locked() if hasattr(request, "lock") else False

                if node_state.status == NodeStatus.PENDING and lock_held:
                    # Node just started
                    node_state.status = NodeStatus.RUNNING
                    node_state.start_time = datetime.now()
                    node_state.last_updated = datetime.now()
                    self.logger.debug(f"Monitor: node {node_id} started")
                    self._notify_status_change(pipeline_id)

                elif node_state.status == NodeStatus.RUNNING and not lock_held:
                    # Node finished – determine outcome by checking cache
                    key = hash(request)
                    cache_exists = (
                        request.source.cache.exists(key) if request.source else False
                    )
                    if cache_exists:
                        node_state.status = NodeStatus.COMPLETED
                        with self._manager_lock:
                            pipeline_state.completed_nodes += 1

                        self.logger.debug(
                            f"Monitor: node {node_id} completed (cache hit)"
                        )
                    else:
                        node_state.status = NodeStatus.SKIPPED
                        node_state.error = (
                            "Execution skipped (no cache entry after lock release)"
                        )
                        self.logger.debug(
                            f"Monitor: node {node_id} skipped (cache miss)"
                        )

                        with self._manager_lock:
                            pipeline_state.completed_nodes += 1

                    node_state.end_time = datetime.now()
                    node_state.last_updated = datetime.now()
                    self._notify_status_change(pipeline_id)

        self.logger.debug(f"Monitor for pipeline {pipeline_id} stopped")

    def _finalize_pipeline(self, pipeline_id: str):
        pipeline_state = self.pipelines[pipeline_id]
        self.logger.info(f"Finalizing pipeline {pipeline_id}")

        # Mark nodes that are still PENDING as SKIPPED if they are downstream of a failed node
        self._mark_skipped_nodes(pipeline_id)

        # Determine final status
        if pipeline_state.status == PipelineStatus.RUNNING:
            if pipeline_state.failed_nodes > 0:
                pipeline_state.status = PipelineStatus.FAILED
                self.logger.warning(
                    f"Pipeline {pipeline_id} finalized with FAILED status"
                )
            else:
                pipeline_state.status = PipelineStatus.COMPLETED
                self.logger.info(
                    f"Pipeline {pipeline_id} finalized with COMPLETED status"
                )

        pipeline_state.completed_at = datetime.now()

        duration = (
            pipeline_state.completed_at - pipeline_state.started_at
        ).total_seconds()
        self.logger.info(f"Pipeline {pipeline_id} completed in {duration:.2f} seconds")
        self.logger.info(
            f"Statistics - Completed: {pipeline_state.completed_nodes}, "
            f"Failed: {pipeline_state.failed_nodes}, "
            f"Cancelled: {pipeline_state.cancelled_nodes}, "
            f"Skipped: {self._count_skipped(pipeline_state)}"
        )

        self._notify_status_change(pipeline_id)

        # Clean up thread references
        with self._manager_lock:
            if pipeline_id in self._pipeline_threads:
                del self._pipeline_threads[pipeline_id]

    def _mark_skipped_nodes(self, pipeline_id: str):
        """BFS from failed nodes to mark all unreachable nodes as SKIPPED."""
        pipeline_state = self.pipelines[pipeline_id]

        # Find all failed nodes
        failed_nodes = {
            node_id
            for node_id, ns in pipeline_state.node_states.items()
            if ns.status == NodeStatus.FAILED
        }

        if not failed_nodes:
            return

        # Propagate to all dependents
        nodes_to_skip = set()
        for failed_node in failed_nodes:
            queue = deque(pipeline_state.node_states[failed_node].dependents)
            while queue:
                dep = queue.popleft()
                if dep not in nodes_to_skip:
                    nodes_to_skip.add(dep)
                    queue.extend(pipeline_state.node_states[dep].dependents)

        # Mark nodes that are still PENDING
        for node_id in nodes_to_skip:
            node_state = pipeline_state.node_states[node_id]
            if node_state.status == NodeStatus.PENDING:
                node_state.status = NodeStatus.SKIPPED
                node_state.error = f"Dependency failed: {failed_nodes}"
                node_state.last_updated = datetime.now()
                pipeline_state.cancelled_nodes += 1

    def _count_skipped(self, pipeline_state: PipelineState) -> int:
        return sum(
            1
            for ns in pipeline_state.node_states.values()
            if ns.status == NodeStatus.SKIPPED
        )

    def cancel_pipeline(self, pipeline_id: str, reason: str = "Cancelled"):
        self.logger.info(f"Cancelling pipeline {pipeline_id}: {reason}")
        with self._manager_lock:
            pipeline_state = self.pipelines.get(pipeline_id)
            if not pipeline_state:
                return

            self._pipeline_stop_flags[pipeline_id] = True

            # Update node states for all nodes that are still PENDING or RUNNING
            for node_state in pipeline_state.node_states.values():
                if node_state.status in {
                    NodeStatus.PENDING,
                    NodeStatus.QUEUED,
                    NodeStatus.RUNNING,
                }:
                    node_state.status = NodeStatus.CANCELLED
                    node_state.error = reason
                    node_state.end_time = datetime.now()
                    node_state.last_updated = datetime.now()
                    pipeline_state.cancelled_nodes += 1

            pipeline_state.status = PipelineStatus.CANCELLED
            pipeline_state.error = reason
            pipeline_state.completed_at = datetime.now()

        self._notify_status_change(pipeline_id)

    # ==================== Control Methods ====================

    def pause_pipeline(self, pipeline_id: str):
        self.logger.info(f"Attempting to pause pipeline {pipeline_id}")
        with self._manager_lock:
            if pipeline_id not in self.pipelines:
                raise ValueError(f"Pipeline {pipeline_id} not found")

            pipeline_state = self.pipelines[pipeline_id]
            if pipeline_state.status != PipelineStatus.RUNNING:
                raise RuntimeError(f"Pipeline {pipeline_id} is not running")

            self._pipeline_pause_events[pipeline_id].clear()
            pipeline_state.status = PipelineStatus.PAUSED

        self._notify_status_change(pipeline_id)
        self.logger.info(f"Pipeline {pipeline_id} paused")

    def resume_pipeline(self, pipeline_id: str):
        self.logger.info(f"Attempting to resume pipeline {pipeline_id}")
        with self._manager_lock:
            if pipeline_id not in self.pipelines:
                raise ValueError(f"Pipeline {pipeline_id} not found")

            pipeline_state = self.pipelines[pipeline_id]
            if pipeline_state.status != PipelineStatus.PAUSED:
                raise RuntimeError(f"Pipeline {pipeline_id} is not paused")

            self._pipeline_pause_events[pipeline_id].set()
            pipeline_state.status = PipelineStatus.RUNNING

        self._notify_status_change(pipeline_id)
        self.logger.info(f"Pipeline {pipeline_id} resumed")

    def stop_pipeline(self, pipeline_id: str):
        self.logger.info(f"Attempting to stop pipeline {pipeline_id}")
        self.cancel_pipeline(pipeline_id, "Stopped by user")

    # ==================== Status Reporting ====================

    def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        with self._manager_lock:
            if pipeline_id not in self.pipelines:
                raise ValueError(f"Pipeline {pipeline_id} not found")
            return self.pipelines[pipeline_id].to_dict()

    def get_all_pipelines_status(self) -> Dict[str, Any]:
        with self._manager_lock:
            return {
                "total_pipelines": len(self.pipelines),
                "pipelines": {
                    pid: state.to_dict() for pid, state in self.pipelines.items()
                },
                "summary": {
                    "running": sum(
                        1
                        for s in self.pipelines.values()
                        if s.status == PipelineStatus.RUNNING
                    ),
                    "paused": sum(
                        1
                        for s in self.pipelines.values()
                        if s.status == PipelineStatus.PAUSED
                    ),
                    "completed": sum(
                        1
                        for s in self.pipelines.values()
                        if s.status == PipelineStatus.COMPLETED
                    ),
                    "failed": sum(
                        1
                        for s in self.pipelines.values()
                        if s.status == PipelineStatus.FAILED
                    ),
                    "cancelled": sum(
                        1
                        for s in self.pipelines.values()
                        if s.status == PipelineStatus.CANCELLED
                    ),
                },
            }

    def get_node_details(self, pipeline_id: str, node_id: str) -> Dict[str, Any]:
        with self._manager_lock:
            if pipeline_id not in self.pipelines:
                raise ValueError(f"Pipeline {pipeline_id} not found")
            pipeline_state = self.pipelines[pipeline_id]
            if node_id not in pipeline_state.node_states:
                raise ValueError(f"Node {node_id} not found")
            node_state = pipeline_state.node_states[node_id]
            details = node_state.to_dict()
            details["pipeline_id"] = pipeline_id
            details["pipeline_status"] = pipeline_state.status.value
            details["batch_position"] = self._find_node_batch(pipeline_state, node_id)
            return details

    def _find_node_batch(
        self, pipeline_state: PipelineState, node_id: str
    ) -> Optional[int]:
        for i, batch in enumerate(pipeline_state.execution_order):
            if node_id in batch:
                return i + 1
        return None

    def register_status_listener(self, callback: callable):
        self._status_listeners.append(callback)

    def _notify_status_change(self, pipeline_id: str):
        with self._manager_lock:
            if pipeline_id in self.pipelines:
                status = self.pipelines[pipeline_id].to_dict()
            else:
                return
        for listener in self._status_listeners:
            try:
                listener(pipeline_id, status)
            except Exception as e:
                self.logger.error(f"Status listener failed: {e}")

    # ==================== Frontend Integration Helpers ====================

    def get_pipeline_visualization_data(self, pipeline_id: str) -> Dict[str, Any]:
        with self._manager_lock:
            if pipeline_id not in self.pipelines:
                raise ValueError(f"Pipeline {pipeline_id} not found")
            pipeline_state = self.pipelines[pipeline_id]

            nodes = []
            for node_id, node_state in pipeline_state.node_states.items():
                nodes.append(
                    {
                        "id": node_id,
                        "label": f"{node_state.source_id}\n{node_id[:8]}...",
                        "status": node_state.status.value,
                        "data": {
                            "source_id": node_state.source_id,
                        },
                    }
                )

            edges = []
            for node_id, node_state in pipeline_state.node_states.items():
                for dep_id in node_state.dependencies:
                    edges.append({"from": dep_id, "to": node_id, "label": "depends_on"})

            timeline = []
            for i, batch in enumerate(pipeline_state.execution_order):
                timeline.append(
                    {
                        "batch": i + 1,
                        "nodes": batch,
                        "status": self._get_batch_status(pipeline_state, batch),
                    }
                )

            return {
                "pipeline_id": pipeline_id,
                "status": pipeline_state.status.value,
                "nodes": nodes,
                "edges": edges,
                "timeline": timeline,
                "metadata": pipeline_state.metadata,
            }

    def _get_batch_status(self, pipeline_state: PipelineState, batch: List[str]) -> str:
        statuses = [pipeline_state.node_states[node_id].status for node_id in batch]
        if any(s == NodeStatus.FAILED for s in statuses):
            return "failed"
        elif all(s == NodeStatus.COMPLETED for s in statuses):
            return "completed"
        elif any(s == NodeStatus.RUNNING for s in statuses):
            return "running"
        elif any(s == NodeStatus.PAUSED for s in statuses):
            return "paused"
        else:
            return "pending"

    # ==================== Utility Methods ====================

    @contextmanager
    def pipeline_context(self, tail_request: "DataRequest", **kwargs):
        pipeline_id = self.register_pipeline(tail_request)
        self.logger.info(f"Pipeline context created for {pipeline_id}")
        try:
            self.run_pipeline(pipeline_id, **kwargs)
            yield pipeline_id
        finally:
            self.logger.info(f"Pipeline context cleanup for {pipeline_id}")
            self.stop_pipeline(pipeline_id)


def sync_run(request):
    """Synchronous wrapper to run a pipeline and wait for completion."""
    manager = PipelineManager()
    manager.start()
    pipeline_id = manager.register_pipeline(request)
    try:
        manager.run_pipeline(pipeline_id)
        # Wait for completion
        while True:
            status = manager.get_pipeline_status(pipeline_id)
            print(f"Progress: {status['progress']['percentage']:.1f}%")
            if status["status"] in {"completed", "failed", "cancelled"}:
                print(f"Final progress: {status['progress']['percentage']:.1f}%")
                break
            time.sleep(2)
    finally:
        manager.stop()
    return status.get("result")
