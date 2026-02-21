from pydantic_settings import BaseSettings
from pydantic import Field


class TelemetrySettings(BaseSettings):
    OTEL_ENABLED: bool = Field(
        description="Boolean indicating whether or not telemetry data is exported via opentelemetry to the observability backend(s).",
        default=False,
    )
    OTEL_SERVICE_NAME: str = Field(
        description="Service identification for opentelemetry.",
        default="hutch-bunny-daemon",
    )
    OTEL_EXPORTER_OTLP_ENDPOINT: str = Field(
        description="Opentelemetry collector endpoint required for sending data.",
        default="http://otel-collector:4317",
    )
