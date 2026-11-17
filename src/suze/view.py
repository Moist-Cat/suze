"""Pipeline visualization: Mermaid and Graphviz.

`dependency_requirements` is the only edge source, and each node's stage list is the only node metadata.
"""

from typing import Optional
from suze.request import DataRequest


def _walk(request: DataRequest, seen: set, out: list):
    if request.request_id in seen:
        return
    seen.add(request.request_id)
    out.append(request)
    for dep in request.dependency_requirements:
        _walk(dep, seen, out)


def to_mermaid(request: DataRequest, *, show_stages: bool = True) -> str:
    nodes: list[DataRequest] = []
    _walk(request, set(), nodes)
    lines = ["graph TD"]

    for req in nodes:
        label = req.source.source_id if req.source else "?"
        if show_stages:
            label += "<br/>" + " → ".join(req.stages.keys() or ["<default>"])
        lines.append(f'  {req.request_id}["{label}"]')

    for req in nodes:
        for dep in req.dependency_requirements:
            lines.append(f"  {dep.request_id} --> {req.request_id}")

    return "\n".join(lines)


def to_graphviz(request: DataRequest, *, show_stages: bool = True):
    try:
        import graphviz
    except ImportError as e:
        raise RuntimeError("to_graphviz requires `graphviz`") from e

    g = graphviz.Digraph()
    g.attr(rankdir="LR")
    nodes: list[DataRequest] = []
    _walk(request, set(), nodes)

    for req in nodes:
        label = req.source.source_id if req.source else "?"
        if show_stages:
            label += "\n" + " → ".join(req.stages.keys() or ["<default>"])
        g.node(req.request_id, label)

    for req in nodes:
        for dep in req.dependency_requirements:
            g.edge(dep.request_id, req.request_id)

    return g


def render(request: DataRequest, *, format: str = "mermaid"):
    if format == "mermaid":
        return to_mermaid(request)
    if format in ("graphviz", "dot"):
        return to_graphviz(request)
    raise ValueError(f"Unknown format: {format}")
