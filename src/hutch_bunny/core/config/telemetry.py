from pydantic_settings import BaseSettings
from pydantic import Field


class TelemetrySettings(BaseSettings):
    """
    Telemetry configuration settings
    """

    OTEL_ENABLED: bool = Field(
        description="Whether to enable OpenTelemetry", default=False
    )
    OTEL_SERVICE_NAME: str = Field(
        description="The service name for OpenTelemetry", default="hutch-bunny"
    )
    OTEL_EXPORTER_OTLP_ENDPOINT: str = Field(
        description="The OTLP exporter endpoint for OpenTelemetry",
        default="http://localhost:4317",
    )


