"""
Latency instrumentation.

Lightweight monotonic-clock histograms for the bot's hot path. Records
microsecond samples per named stage and computes p50/p95/p99/max on demand.

Usage:
    from core import latency

    t0 = time.monotonic_ns()
    # ... do work ...
    latency.record("recv_to_analyze", (time.monotonic_ns() - t0) // 1000)

    snapshot = latency.snapshot_all()
    latency.dump_json("logs/latency.json")

Designed to be cheap enough to call on every market update: O(1) record,
bounded memory via a ring buffer of N most-recent samples per stage.
"""

import json
import os
import threading
from typing import Optional


# Per-stage sample cap. 10_000 samples × ~10 stages × 8 bytes ≈ 800 KB worst case.
_MAX_SAMPLES = 10_000


class LatencyHistogram:
    """Ring buffer of microsecond samples with percentile snapshots."""

    __slots__ = ("name", "_samples", "_lock", "_total_count", "_total_sum_us")

    def __init__(self, name: str):
        self.name = name
        self._samples: list[int] = []
        self._lock = threading.Lock()
        self._total_count = 0
        self._total_sum_us = 0

    def record(self, microseconds: int) -> None:
        if microseconds < 0:
            return
        with self._lock:
            self._total_count += 1
            self._total_sum_us += microseconds
            if len(self._samples) < _MAX_SAMPLES:
                self._samples.append(microseconds)
            else:
                self._samples[self._total_count % _MAX_SAMPLES] = microseconds

    def snapshot(self) -> dict:
        with self._lock:
            samples = sorted(self._samples)
            total = self._total_count
            sum_us = self._total_sum_us
        if not samples:
            return {
                "name": self.name,
                "count": 0,
                "p50_us": 0,
                "p95_us": 0,
                "p99_us": 0,
                "max_us": 0,
                "mean_us": 0,
            }
        n = len(samples)
        return {
            "name": self.name,
            "count": total,
            "p50_us": samples[n // 2],
            "p95_us": samples[min(n - 1, int(n * 0.95))],
            "p99_us": samples[min(n - 1, int(n * 0.99))],
            "max_us": samples[-1],
            "mean_us": sum_us // total if total else 0,
        }

    def reset(self) -> None:
        with self._lock:
            self._samples.clear()
            self._total_count = 0
            self._total_sum_us = 0


_registry: dict[str, LatencyHistogram] = {}
_registry_lock = threading.Lock()


def _get(name: str) -> LatencyHistogram:
    h = _registry.get(name)
    if h is not None:
        return h
    with _registry_lock:
        h = _registry.get(name)
        if h is None:
            h = LatencyHistogram(name)
            _registry[name] = h
        return h


def record(name: str, microseconds: int) -> None:
    _get(name).record(microseconds)


def snapshot_all() -> list[dict]:
    with _registry_lock:
        names = list(_registry.keys())
    return [_registry[n].snapshot() for n in names]


def dump_json(path: str) -> None:
    snapshots = snapshot_all()
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(tmp, "w") as f:
        json.dump({"histograms": snapshots}, f, indent=2)
    os.replace(tmp, path)


def reset_all() -> None:
    with _registry_lock:
        for h in _registry.values():
            h.reset()


def summary_line() -> str:
    """One-line human summary for logging — p50/p99 per stage."""
    parts = []
    for s in snapshot_all():
        if s["count"] == 0:
            continue
        parts.append(f"{s['name']}: p50={s['p50_us']}μs p99={s['p99_us']}μs n={s['count']}")
    return " | ".join(parts) if parts else "no samples"
