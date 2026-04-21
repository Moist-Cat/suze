from pathlib import Path
import os
import sys
import logging
import logging.config

from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
    ConsoleMetricExporter,
)
import opentelemetry.instrumentation.instrumentor as otel_instrumentor
from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor

APP_VERBOSE_NAME = "Suze - E.T.L. Framework"
APP_NAME = "suze"
BASE_DIR = Path(__file__).parent
CACHE_DIR = Path(os.getenv("SUZE_CACHE_DIR", BASE_DIR / "cache"))

# Config
DEBUG = False
FILE_LOGGING = os.getenv("SUZE_FILELOGGING", True)
if isinstance(FILE_LOGGING, str) and not FILE_LOGGING.strip():
    FILE_LOGGING = False
DISABLE = True

# Initialize OpenTelemetry once (singleton pattern)
_otel_initialized = False


def init_opentelemetry(service_name: str = "suze"):
    """Initialize OpenTelemetry with OTLP exporter"""
    global _otel_initialized

    if _otel_initialized:
        return

    # Create resource
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": "1.0.0",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.language": "python",
        }
    )

    # Initialize tracing
    if not DISABLE:
        trace_provider = TracerProvider(resource=resource)
        span_exporter = OTLPSpanExporter() if DEBUG is False else ConsoleSpanExporter()
        trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
        trace.set_tracer_provider(trace_provider)

        # Initialize metrics
        metric_exporter = (
            OTLPMetricExporter() if DEBUG is False else ConsoleMetricExporter()
        )
        reader = PeriodicExportingMetricReader(
            exporter=metric_exporter, export_interval_millis=5000
        )
        meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(meter_provider)

    # asyncio

    _otel_initialized = True


init_opentelemetry(service_name=APP_NAME)

# Logging
LOGGERS = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "basic": {
            "style": "{",
            "format": "{asctime:s} [{levelname:s}] -- {name:s}: {message:s}",
        },
        "detailed": {
            "style": "{",
            "format": "{asctime:s} [{levelname:s}] [{name:s}] [{funcName:s}:{lineno:d}] -- {message:s}",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": sys.stderr,
            "formatter": "basic",
            "level": "DEBUG" if DEBUG else "INFO",
        },
        "audit_file": (
            {
                "class": "logging.handlers.RotatingFileHandler",
                "maxBytes": 5000000,
                "backupCount": 5,
                "filename": BASE_DIR / "logs" / "api.error.log",
                "encoding": "utf-8",
                "formatter": "detailed",
                "level": "ERROR",
                # for read-only filesystems
            }
            if FILE_LOGGING
            else {
                "class": "logging.StreamHandler",
                "stream": sys.stderr,
                "formatter": "basic",
                "level": "ERROR",
            }
        ),
    },
    "loggers": {
        "global": {
            "handlers": ["console", "audit_file"],
            "level": "DEBUG" if DEBUG else "INFO",
            "propagate": False,
        },
        "user_info": {
            "handlers": ["console", "audit_file"],
            "level": "DEBUG" if DEBUG else "INFO",
            "propagate": False,
        },
        "audit": {
            "handlers": ["audit_file", "console"] if DEBUG else ["audit_file"],
            "level": "INFO",
            "propagate": False,
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "WARNING",
    },
}

logging.config.dictConfig(LOGGERS)
