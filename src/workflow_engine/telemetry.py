"""OpenTelemetry setup, plus trace-context propagation through stream messages.

A tracer provider is always installed, so trace IDs exist for log correlation and are carried
from the API to the worker inside each stream entry. Spans are only *exported* (OTLP over HTTP,
e.g. to Jaeger) when OTEL_ENABLED is true; SQLAlchemy and Redis auto-instrumentation is also
only switched on then, since those spans are the noisy, costly part.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI
from opentelemetry import context, propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from sqlalchemy.ext.asyncio import AsyncEngine

from workflow_engine.config import Settings

TRACER_NAME = "workflow_engine"
# Only W3C trace-context keys are copied between stream fields and the propagator.
TRACE_FIELDS = ("traceparent", "tracestate")

_provider: TracerProvider | None = None
_instrumented: set[str] = set()


def configure_tracing(settings: Settings, component: str) -> TracerProvider:
    """Install the global tracer provider once per process (later calls return it)."""
    global _provider
    if _provider is not None:
        return _provider
    resource = Resource.create(
        {
            "service.name": f"{settings.otel_service_name}-{component}",
            "service.namespace": settings.otel_service_name,
        }
    )
    provider = TracerProvider(resource=resource)
    if settings.otel_enabled:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        endpoint = settings.otel_exporter_otlp_endpoint.rstrip("/") + "/v1/traces"
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    _provider = provider
    return provider


def shutdown_tracing() -> None:
    """Flush pending spans on process exit."""
    if _provider is not None:
        _provider.shutdown()


def tracer() -> trace.Tracer:
    return trace.get_tracer(TRACER_NAME)


def instrument_app(app: FastAPI) -> None:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app, excluded_urls="health,metrics")


def instrument_clients(settings: Settings, engine: AsyncEngine) -> None:
    """SQLAlchemy + Redis auto-instrumentation, only when spans are exported."""
    if not settings.otel_enabled:
        return
    if "sqlalchemy" not in _instrumented:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        # The instrumentor's declared range stops below SQLAlchemy 2.1, but the engine events
        # it hooks are unchanged there (covered by tests/integration/test_observability.py).
        SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine, skip_dep_check=True)
        _instrumented.add("sqlalchemy")
    if "redis" not in _instrumented:
        from opentelemetry.instrumentation.redis import RedisInstrumentor

        RedisInstrumentor().instrument()
        _instrumented.add("redis")


def inject_trace_context() -> dict[str, str]:
    """The current trace context as stream-entry fields (empty when no span is active)."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return {k: v for k, v in carrier.items() if k in TRACE_FIELDS}


def extract_trace_context(fields: Mapping[str, Any]) -> context.Context:
    carrier = {k: str(fields[k]) for k in TRACE_FIELDS if k in fields}
    return propagate.extract(carrier)


def current_trace_id() -> str | None:
    ctx = trace.get_current_span().get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid else None
