"""Unit tests for core/latency.py."""

import json
import os

import pytest

from core import latency
from core.latency import LatencyHistogram


@pytest.fixture(autouse=True)
def _reset():
    latency.reset_all()
    yield
    latency.reset_all()


def test_empty_snapshot():
    h = LatencyHistogram("empty")
    snap = h.snapshot()
    assert snap["count"] == 0
    assert snap["p50_us"] == 0
    assert snap["p99_us"] == 0
    assert snap["max_us"] == 0


def test_record_and_snapshot_basic():
    h = LatencyHistogram("basic")
    for v in [10, 20, 30, 40, 50]:
        h.record(v)
    snap = h.snapshot()
    assert snap["count"] == 5
    assert snap["max_us"] == 50
    # p50 with n=5 sorted [10,20,30,40,50] → index 2 → 30
    assert snap["p50_us"] == 30
    assert snap["mean_us"] == 30


def test_percentiles_at_scale():
    h = LatencyHistogram("scale")
    for v in range(1, 1001):
        h.record(v)
    snap = h.snapshot()
    assert snap["count"] == 1000
    # n=1000 sorted [1..1000] → p50 at index 500 → 501
    assert snap["p50_us"] == 501
    # p95 at index 950 → 951
    assert snap["p95_us"] == 951
    # p99 at index 990 → 991
    assert snap["p99_us"] == 991
    assert snap["max_us"] == 1000


def test_negative_values_ignored():
    h = LatencyHistogram("neg")
    h.record(-1)
    h.record(100)
    snap = h.snapshot()
    assert snap["count"] == 1
    assert snap["max_us"] == 100


def test_reset_clears():
    h = LatencyHistogram("rst")
    for v in [10, 20, 30]:
        h.record(v)
    h.reset()
    snap = h.snapshot()
    assert snap["count"] == 0


def test_registry_record_and_snapshot_all():
    latency.record("stage_a", 100)
    latency.record("stage_a", 200)
    latency.record("stage_b", 50)

    snaps = {s["name"]: s for s in latency.snapshot_all()}
    assert "stage_a" in snaps and "stage_b" in snaps
    assert snaps["stage_a"]["count"] == 2
    assert snaps["stage_b"]["count"] == 1


def test_dump_json_roundtrip(tmp_path):
    latency.record("foo", 123)
    latency.record("foo", 456)
    out = str(tmp_path / "latency.json")
    latency.dump_json(out)

    assert os.path.exists(out)
    with open(out) as f:
        data = json.load(f)
    assert "histograms" in data
    foo = next(h for h in data["histograms"] if h["name"] == "foo")
    assert foo["count"] == 2


def test_summary_line_format():
    latency.record("recv_to_analyze", 250)
    latency.record("recv_to_analyze", 350)
    line = latency.summary_line()
    assert "recv_to_analyze" in line
    assert "p50=" in line
    assert "p99=" in line


def test_summary_line_no_samples():
    assert latency.summary_line() == "no samples"
