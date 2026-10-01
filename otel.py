"""Best-effort OpenTelemetry HTTP telemetry for the activity service."""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any


class _Noop:
    def add(self, *_: Any, **__: Any) -> None: return
    def record(self, *_: Any, **__: Any) -> None: return


@dataclass
class Request:
    telemetry: "Telemetry"
    method: str
    path: str
    started: float
    span: Any = None
    finished: bool = False

    def finish(self, status: int, route: str | None = None) -> None:
        if self.finished: return
        self.finished = True
        attrs = {"http.request.method": self.method, "http.route": route or self.path, "http.response.status_code": status}
        try:
            self.telemetry.requests.add(1, attrs)
            self.telemetry.duration.record((time.perf_counter() - self.started) * 1000, attrs)
            if self.span:
                self.span.set_attribute("http.request.method", self.method)
                self.span.set_attribute("url.path", self.path)
                self.span.set_attribute("http.route", route or self.path)
                self.span.set_attribute("http.response.status_code", status)
                self.span.end()
        except Exception: return


class Telemetry:
    def __init__(self, tracer: Any = None, meter: Any = None) -> None:
        self.tracer = tracer
        self.requests = meter.create_counter("myota.http.server.requests", unit="{request}") if meter else _Noop()
        self.duration = meter.create_histogram("myota.http.server.duration", unit="ms") if meter else _Noop()

    def start_request(self, method: str, path: str) -> Request:
        span = None
        try:
            if self.tracer: span = self.tracer.start_span(f"{method} {path}")
        except Exception: pass
        return Request(self, method, path, time.perf_counter(), span)


_lock = threading.Lock()
_instances: dict[str, Telemetry] = {}


def telemetry_for(service_name: str) -> Telemetry:
    with _lock:
        if service_name in _instances: return _instances[service_name]
        if os.environ.get("MYOTA_OTEL_ENABLED", "0").strip().lower() in {"", "0", "false", "no", "off"}:
            value = Telemetry()
        else:
            try:
                from opentelemetry import metrics, trace
                from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                from opentelemetry.sdk.metrics import MeterProvider
                from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
                from opentelemetry.sdk.resources import Resource
                from opentelemetry.sdk.trace import TracerProvider
                from opentelemetry.sdk.trace.export import BatchSpanProcessor
                endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
                resource = Resource.create({"service.name": service_name, "service.namespace": "myota", "deployment.environment": os.environ.get("MYOTA_ENV", "development")})
                provider = TracerProvider(resource=resource)
                provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=not endpoint.startswith("https://"))))
                trace.set_tracer_provider(provider)
                reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=endpoint, insecure=not endpoint.startswith("https://")), export_interval_millis=int(os.environ.get("MYOTA_OTEL_METRIC_INTERVAL_MS", "15000")))
                metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))
                value = Telemetry(trace.get_tracer("myota.http"), metrics.get_meter("myota.http"))
            except Exception:
                value = Telemetry()
        _instances[service_name] = value
        return value
