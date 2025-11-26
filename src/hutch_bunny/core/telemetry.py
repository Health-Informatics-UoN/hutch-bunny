import time 
from functools import wraps
from typing import Callable, TypeVar, ParamSpec
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan 
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from importlib.metadata import version
from sqlalchemy import event
from sqlalchemy.engine import Engine

from hutch_bunny.core.settings import Settings 
from hutch_bunny.core.logger import logger


P = ParamSpec("P")
R = TypeVar("R")


# ============================================================================
# Setup Functions
# ============================================================================


def setup_telemetry(settings: Settings) -> None: 
    """Minimal telemetry setup"""

    if not settings.OTEL_ENABLED: 
        return 
    
    try:
        resource = Resource.create({
            SERVICE_NAME: settings.OTEL_SERVICE_NAME,
            SERVICE_VERSION: version("hutch-bunny"),
        })

        _setup_tracing(resource, settings)

        _setup_logging_integration(resource, settings)

        _setup_metrics(resource, settings)

        SQLAlchemyInstrumentor().instrument()
        RequestsInstrumentor().instrument()

        _create_metrics()  
        _instrument_sqlalchemy_metrics()  


        print("OpenTelemetry initialized!")
        
    except Exception as e:
        print(f"OpenTelemetry setup failed: {e}")


def _setup_tracing(resource: Resource, settings: Settings) -> None:
    """Setup distributed tracing."""
    trace_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(trace_provider)

    trace_exporter = OTLPSpanExporter(
        endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT
    )
    trace_provider.add_span_processor(DropPollingSpansProcessor(trace_exporter))


def _setup_metrics(resource: Resource, settings: Settings) -> None: 
    """Setup metrics collection."""
    metric_exporter = OTLPMetricExporter(
        endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT
    )
    metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=10000)
    metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(metric_provider)


def _setup_logging_integration(resource: Resource, settings: Settings) -> None: 
    """Setup logging integration with existing Bunny logger."""
    log_provider = LoggerProvider(resource=resource)
    set_logger_provider(log_provider)

    log_exporter = OTLPLogExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT)
    log_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

    otel_handler = LoggingHandler(logger_provider=log_provider)
    logger.addHandler(otel_handler)


# ============================================================================
# Decorators
# ============================================================================


def trace_operation(operation_name: str, span_kind: trace.SpanKind = trace.SpanKind.INTERNAL) -> Callable:
    """Decorator to trace function execution with minimal code invasion."""
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        tracer = trace.get_tracer(f"hutch-bunny.{func.__module__}")
        
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with tracer.start_as_current_span(
                operation_name or func.__name__, 
                kind=span_kind
            ) as span:
                try:
                    result = func(*args, **kwargs)
                    span.set_status(trace.Status(trace.StatusCode.OK))
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    raise
        return wrapper
    return decorator


# ============================================================================
# Database Metrics (Auto-instrumented via SQLAlchemy Events)
# ============================================================================


def _create_metrics() -> None:
    """Create all metrics AFTER meter provider is set."""
    global db_query_counter, db_query_duration_histogram
    
    meter = metrics.get_meter("hutch-bunny")
    
    db_query_counter = meter.create_counter(
        name="bunny_db_queries_total",
        description="Total number of database queries executed",
        unit="1",
    )
    
    db_query_duration_histogram = meter.create_histogram(
        name="bunny_db_query_duration_seconds",
        description="Time spent executing database queries",
        unit="s",
    )


def _instrument_sqlalchemy_metrics() -> None:
    """
    Instrument SQLAlchemy with custom metrics using event listeners.
    This automatically captures all database queries without code changes.
    """
    from typing import Any
    
    @event.listens_for(Engine, "before_cursor_execute")
    def before_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool
    ) -> None:
        """Record the start time before query execution."""
        context._query_start_time = time.time()
    
    @event.listens_for(Engine, "after_cursor_execute")
    def after_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool
    ) -> None:
        """Record metrics after query execution."""
        duration = time.time() - context._query_start_time
        
        # Extract operation type (SELECT, INSERT, UPDATE, DELETE, etc.)
        operation = statement.strip().split()[0].upper() if statement else "UNKNOWN"
        
        # Record metrics with operation label
        db_query_counter.add(1, {"operation": operation})
        db_query_duration_histogram.record(duration, {"operation": operation})


class DropPollingSpansProcessor(BatchSpanProcessor):
    def on_end(self, span: ReadableSpan) -> None:
        attributes = span.attributes
        if attributes is None:
            return 
        url_value = attributes.get("http.url")
        if isinstance(url_value, str) and "/task/nextjob/" in url_value:
            return  
        super().on_end(span)