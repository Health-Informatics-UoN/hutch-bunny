import os
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
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

from hutch_bunny.core.settings import Settings 
from hutch_bunny.core.logger import logger


def setup_telemetry(settings: Settings) -> None: 
    """Minimal telemetry setup"""

    if not settings.OTEL_ENABLED: 
        return 
    
    try:
        resource = Resource.create({
            SERVICE_NAME: settings.OTEL_SERVICE_NAME,
            SERVICE_VERSION: version("hutch-bunny"),
        })

        trace_provider = TracerProvider(resource=resource)
        trace.set_tracer_provider(trace_provider)

        trace_exporter = OTLPSpanExporter(
            endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT
        )
        trace_provider.add_span_processor(BatchSpanProcessor(trace_exporter))

        metric_exporter = OTLPMetricExporter(
            endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT
        )
        metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=10000)
        metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(metric_provider)

        log_provider = LoggerProvider(resource=resource)
        set_logger_provider(log_provider)
        log_exporter = OTLPLogExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT)
        log_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
        otel_handler = LoggingHandler(logger_provider=log_provider)
        logger.addHandler(otel_handler)

        SQLAlchemyInstrumentor().instrument()
        RequestsInstrumentor().instrument()

        print("OpenTelemetry initialized!")
        
    except Exception as e:
        print(f"OpenTelemetry setup failed: {e}")




