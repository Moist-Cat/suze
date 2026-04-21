import contextlib
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry.trace.span import INVALID_SPAN_ID, INVALID_TRACE_ID


def create_noop_span():
    """Create a no-op span"""
    span_context = SpanContext(
        trace_id=INVALID_TRACE_ID,
        span_id=INVALID_SPAN_ID,
        is_remote=False,
        trace_flags=TraceFlags(0),
        trace_state=None,
    )
    return NonRecordingSpan(span_context)


@contextlib.contextmanager
def start_noop_span(name="noop-span", attributes=None):
    """
    Context manager that yields a no-op span

    Args:
        name: Span name (ignored, but kept for API compatibility)
        attributes: Span attributes (ignored)
    """
    span = create_noop_span()
    try:
        yield span
    finally:
        span.end()  # No-op
