"""Small dependency-free Prometheus registry used by the standalone service."""

from __future__ import annotations

import threading
from collections import defaultdict
from typing import Mapping


def _labels(labels: Mapping[str, object]) -> str:
    if not labels:
        return ""
    return (
        "{"
        + ",".join(
            f'{key}="{str(labels[key]).replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"'
            for key in sorted(labels)
        )
        + "}"
    )


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counters: defaultdict[
            tuple[str, tuple[tuple[str, str], ...]], float
        ] = defaultdict(float)

    def inc(
        self, name: str, labels: Mapping[str, object] | None = None
    ) -> None:
        key = (
            name,
            tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items())),
        )
        with self._lock:
            self._counters[key] += 1

    def render(self, extra: Mapping[str, float] | None = None) -> str:
        with self._lock:
            rows = list(self._counters.items())
        lines = [
            f"{name}{_labels(dict(labels))} {value:g}"
            for (name, labels), value in sorted(rows)
        ]
        lines += [
            f"{name} {float(value):g}"
            for name, value in sorted((extra or {}).items())
        ]
        return "\n".join(lines) + "\n"


METRICS = MetricsRegistry()
