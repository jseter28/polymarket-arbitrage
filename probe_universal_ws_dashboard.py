"""
24-hour Universal WS soak probe with live web dashboard.

Runs PolymarketUniversalWS for a configurable duration and exposes a live
dashboard at http://localhost:8889 showing:
  - Aggregate metrics (msg/sec, queue depth, drops, alive shards)
  - Timeline charts (msg/sec, drops/sec, queue depth, total sessions)
  - Per-shard table (state, last error — retained even after recovery)
  - Event log (chronological shard state transitions and reconnects)

Usage:
    python3 probe_universal_ws_dashboard.py --duration 86400 \\
        --output probe_universal_24h.json --port 8889
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from polymarket_client.universal_ws import PolymarketUniversalWS

logger = logging.getLogger("probe_ws_dashboard")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# History recorder
# ---------------------------------------------------------------------------


class HistoryRecorder:
    """Polls ws.status() at fixed period; builds rolling history + event log."""

    def __init__(
        self,
        ws: PolymarketUniversalWS,
        period_s: float = 2.0,
        max_samples: int = 43200,  # 24h at 2s sampling
        max_events: int = 10000,
    ):
        self.ws = ws
        self.period_s = period_s
        self.samples: deque[dict] = deque(maxlen=max_samples)
        self.events: deque[dict] = deque(maxlen=max_events)
        self._last_shard_states: dict[int, dict] = {}
        self._last_msg_total: int = 0
        self._last_drops: int = 0
        self._start_mono: float = 0.0
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self.probe_meta: dict = {}

    def set_probe_meta(self, **kwargs) -> None:
        self.probe_meta.update(kwargs)

    async def start(self) -> None:
        self._start_mono = time.monotonic()
        self._task = asyncio.create_task(self._run(), name="history_recorder")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self.period_s)
                    return
                except asyncio.TimeoutError:
                    pass
                try:
                    self._take_sample()
                except Exception as e:
                    logger.warning(f"recorder sample failed: {e}")
        except asyncio.CancelledError:
            raise

    def _take_sample(self) -> None:
        status = self.ws.status()
        now_mono = time.monotonic()
        elapsed = now_mono - self._start_mono

        shards = status["shards"]
        msg_total = sum(s["message_count"] for s in shards)
        sess_total = sum(s["session_count"] for s in shards)
        alive = sum(1 for s in shards if s["state"] == "connected")
        quarantined = sum(1 for s in shards if s["state"] == "quarantined")
        first_msg = sum(1 for s in shards if s["first_msg_received"])

        msgs_delta = msg_total - self._last_msg_total
        drops_delta = status["drops"] - self._last_drops
        msgs_per_sec = max(0.0, msgs_delta / self.period_s)
        drops_per_sec = max(0.0, drops_delta / self.period_s)

        self.samples.append({
            "t": round(elapsed, 1),
            "msgs_per_sec": round(msgs_per_sec, 1),
            "drops_per_sec": round(drops_per_sec, 1),
            "queue_depth": status["queue_depth"],
            "alive_shards": alive,
            "quarantined_shards": quarantined,
            "first_msg_shards": first_msg,
            "total_messages": msg_total,
            "total_drops": status["drops"],
            "total_sessions": sess_total,
        })
        self._last_msg_total = msg_total
        self._last_drops = status["drops"]

        for s in shards:
            sid = s["shard_id"]
            prev = self._last_shard_states.get(sid)
            cur_state = s["state"]
            cur_sessions = s["session_count"]
            reason = s["last_disconnect_reason"] or s["last_open_failure"]
            if prev is None:
                self.events.append({
                    "t": round(elapsed, 1),
                    "wall_utc": utcnow_iso(),
                    "shard_id": sid,
                    "kind": "init",
                    "state": cur_state,
                })
            else:
                if prev["state"] != cur_state:
                    self.events.append({
                        "t": round(elapsed, 1),
                        "wall_utc": utcnow_iso(),
                        "shard_id": sid,
                        "kind": "state_change",
                        "from_state": prev["state"],
                        "to_state": cur_state,
                        "reason": reason,
                    })
                if cur_sessions > prev.get("session_count", 0):
                    self.events.append({
                        "t": round(elapsed, 1),
                        "wall_utc": utcnow_iso(),
                        "shard_id": sid,
                        "kind": "session_started",
                        "session_count": cur_sessions,
                        "reason": reason,
                    })
            self._last_shard_states[sid] = {
                "state": cur_state,
                "session_count": cur_sessions,
            }

    def snapshot(self) -> dict:
        status = self.ws.status()
        elapsed = time.monotonic() - self._start_mono if self._start_mono else 0
        msg_1min, msg_10min = self._compute_recent_rates()
        shards = status["shards"]
        return {
            "probe": {
                **self.probe_meta,
                "elapsed_s": round(elapsed, 1),
                "wall_utc": utcnow_iso(),
            },
            "aggregate": {
                "shard_count": status["shard_count"],
                "market_count": status["market_count"],
                "token_count": status["token_count"],
                "queue_depth": status["queue_depth"],
                "queue_maxsize": status["queue_maxsize"],
                "drops": status["drops"],
                "total_messages": sum(s["message_count"] for s in shards),
                "total_sessions": sum(s["session_count"] for s in shards),
                "alive_shards": sum(1 for s in shards if s["state"] == "connected"),
                "quarantined_shards": sum(1 for s in shards if s["state"] == "quarantined"),
                "first_msg_shards": sum(1 for s in shards if s["first_msg_received"]),
                "msg_per_sec_1min": msg_1min,
                "msg_per_sec_10min": msg_10min,
            },
            "shards": shards,
        }

    def _compute_recent_rates(self) -> tuple[float, float]:
        if not self.samples:
            return 0.0, 0.0
        n_1min = max(1, int(60 / self.period_s))
        n_10min = max(1, int(600 / self.period_s))
        recent = list(self.samples)
        r1 = recent[-n_1min:]
        r10 = recent[-n_10min:]
        avg1 = sum(s["msgs_per_sec"] for s in r1) / len(r1) if r1 else 0.0
        avg10 = sum(s["msgs_per_sec"] for s in r10) / len(r10) if r10 else 0.0
        return round(avg1, 1), round(avg10, 1)

    def history(self, bucket_s: int = 60) -> list[dict]:
        if not self.samples:
            return []
        n_per_bucket = max(1, int(bucket_s / self.period_s))
        out: list[dict] = []
        buf: list[dict] = []
        for s in self.samples:
            buf.append(s)
            if len(buf) >= n_per_bucket:
                out.append(self._bucket_summary(buf))
                buf = []
        if buf:
            out.append(self._bucket_summary(buf))
        return out

    @staticmethod
    def _bucket_summary(buf: list[dict]) -> dict:
        return {
            "t": buf[-1]["t"],
            "msgs_per_sec": round(sum(s["msgs_per_sec"] for s in buf) / len(buf), 1),
            "drops_per_sec": round(sum(s["drops_per_sec"] for s in buf) / len(buf), 1),
            "queue_depth": max(s["queue_depth"] for s in buf),
            "alive_shards": buf[-1]["alive_shards"],
            "quarantined_shards": buf[-1]["quarantined_shards"],
            "total_sessions": buf[-1]["total_sessions"],
        }


# ---------------------------------------------------------------------------
# Dashboard HTML
# ---------------------------------------------------------------------------

DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Universal WS Soak</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0b0f14;
    --bg2: #131922;
    --bg3: #1b2330;
    --fg: #e6edf3;
    --muted: #8b96a5;
    --accent: #2dd4bf;
    --warn: #fbbf24;
    --danger: #ef4444;
    --good: #4ade80;
  }
  * { box-sizing: border-box; }
  body {
    background: var(--bg); color: var(--fg);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
    margin: 0; padding: 16px;
    font-size: 13px;
  }
  header { display: flex; flex-direction: column; gap: 6px; margin-bottom: 16px; }
  header h1 { margin: 0; font-size: 18px; font-weight: 600; }
  .meta { display: flex; gap: 16px; flex-wrap: wrap; color: var(--muted); font-size: 12px; }
  .meta b { color: var(--fg); font-weight: 500; }
  .fatal { color: var(--danger); font-weight: 600; }
  .hidden { display: none; }

  .tiles {
    display: grid; gap: 8px;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    margin-bottom: 16px;
  }
  .tile { background: var(--bg2); border-radius: 6px; padding: 10px 12px; border: 1px solid var(--bg3); }
  .tile .label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }
  .tile .value { font-size: 22px; font-weight: 600; margin-top: 4px; font-variant-numeric: tabular-nums; }
  .tile .sub { font-size: 11px; color: var(--muted); margin-top: 2px; }
  .tile.warn .value { color: var(--warn); }
  .tile.danger .value { color: var(--danger); }
  .tile.good .value { color: var(--good); }

  .charts {
    display: grid; gap: 12px;
    grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
    margin-bottom: 16px;
  }
  .chart-box { background: var(--bg2); border-radius: 6px; padding: 12px; border: 1px solid var(--bg3); }
  .chart-box h3 { margin: 0 0 8px 0; font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; font-weight: 500; }
  .chart-box canvas { max-height: 200px; }

  section h2 { font-size: 14px; margin: 0 0 8px 0; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; font-weight: 500; }
  .panel { background: var(--bg2); border-radius: 6px; border: 1px solid var(--bg3); margin-bottom: 16px; overflow: hidden; }

  table { width: 100%; border-collapse: collapse; font-size: 12px; font-variant-numeric: tabular-nums; }
  th, td { padding: 6px 10px; text-align: left; border-bottom: 1px solid var(--bg3); }
  th { background: var(--bg3); color: var(--muted); text-transform: uppercase; font-size: 11px; font-weight: 500; letter-spacing: 0.5px; }
  tr.shard-dead td { color: var(--danger); }
  tr.shard-dead td.state { font-weight: 600; }
  tr.shard-quar td { color: var(--warn); }
  tr.shard-recon td.state { color: var(--warn); }
  tr.shard-conn td.state { color: var(--good); }
  td.err { max-width: 360px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--muted); font-size: 11px; }

  .events { max-height: 340px; overflow-y: auto; padding: 0; margin: 0; list-style: none; font-size: 12px; }
  .events li { padding: 4px 12px; border-bottom: 1px solid var(--bg3); font-variant-numeric: tabular-nums; display: flex; gap: 12px; }
  .events li .t { color: var(--muted); min-width: 80px; }
  .events li .sid { min-width: 60px; color: var(--accent); }
  .events li .kind { min-width: 110px; }
  .events li .kind.state_change { color: var(--warn); }
  .events li .kind.session_started { color: var(--accent); }
  .events li .reason { color: var(--muted); }
  .events li.danger .kind { color: var(--danger); }

  /* ---------- Top nav (always visible) ---------- */
  .topbar { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 12px; }
  .topbar h1 { margin: 0; font-size: 16px; font-weight: 600; }
  .nav-tabs { display: flex; gap: 4px; }
  .nav-tabs a {
    display: inline-block;
    padding: 6px 14px;
    color: var(--muted);
    text-decoration: none;
    border: 1px solid var(--bg3);
    border-radius: 4px;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    background: var(--bg2);
  }
  .nav-tabs a:hover { color: var(--fg); }
  .nav-tabs a.active { color: var(--fg); border-color: var(--accent); background: var(--bg3); }
  .topbar .clock { color: var(--muted); font-size: 12px; font-variant-numeric: tabular-nums; }
  .topbar .clock b { color: var(--fg); font-weight: 500; }

  /* ---------- Markets view layout ---------- */
  #view-markets { display: grid; grid-template-columns: 220px 1fr; gap: 12px; align-items: start; }
  .sidebar {
    background: var(--bg2); border: 1px solid var(--bg3); border-radius: 6px;
    padding: 10px 0; max-height: calc(100vh - 110px); overflow-y: auto;
    position: sticky; top: 16px;
  }
  .sidebar h3 { margin: 0 0 8px 0; padding: 0 14px; font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; font-weight: 500; }
  .sidebar ul { list-style: none; padding: 0; margin: 0; }
  .sidebar li a {
    display: flex; justify-content: space-between; align-items: baseline;
    padding: 5px 14px; color: var(--fg); text-decoration: none;
    font-size: 12px; border-left: 2px solid transparent;
  }
  .sidebar li a:hover { background: var(--bg3); }
  .sidebar li a.active { background: var(--bg3); border-left-color: var(--accent); color: var(--fg); }
  .sidebar li a .count { color: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; }
  .sidebar li a.active .count { color: var(--accent); }
  .sidebar .empty { padding: 12px 14px; color: var(--muted); font-size: 12px; font-style: italic; }

  .grid-wrap { min-width: 0; }
  .grid-header {
    display: flex; justify-content: space-between; align-items: baseline;
    margin-bottom: 8px;
  }
  .grid-header h2 { margin: 0; font-size: 14px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; font-weight: 500; }
  .grid-header .grid-meta { color: var(--muted); font-size: 11px; }
  .markets-panel { background: var(--bg2); border: 1px solid var(--bg3); border-radius: 6px; overflow: hidden; }
  #markets-table { width: 100%; border-collapse: collapse; font-size: 12px; font-variant-numeric: tabular-nums; }
  #markets-table th { background: var(--bg3); color: var(--muted); text-transform: uppercase; font-size: 11px; font-weight: 500; letter-spacing: 0.5px; padding: 6px 10px; text-align: left; cursor: default; user-select: none; }
  #markets-table th.num { text-align: right; }
  #markets-table td { padding: 6px 10px; border-bottom: 1px solid var(--bg3); }
  #markets-table td.num { text-align: right; }
  #markets-table tr.stale td { color: var(--muted); }
  #markets-table td.q { max-width: 480px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  #markets-table .nobook { color: var(--muted); font-style: italic; }
  #markets-table .empty { padding: 16px; text-align: center; color: var(--muted); font-style: italic; }
</style>
</head>
<body>

<div class="topbar">
  <h1>Universal WS Soak</h1>
  <nav class="nav-tabs">
    <a href="#status" id="tab-status">Status</a>
    <a href="#markets" id="tab-markets">Markets</a>
  </nav>
  <div class="clock"><b id="elapsed">…</b> <span id="elapsed-pct"></span></div>
</div>

<div id="view-status">

<header>
  <div class="meta">
    <span>start: <b id="start-time">…</b></span>
    <span>target: <b id="target-duration">…</b></span>
    <span>shard size: <b id="shard-size">…</b></span>
    <span>updated: <b id="last-updated">…</b></span>
    <span id="fatal-wrap" class="fatal hidden">FATAL: <span id="fatal-msg"></span></span>
  </div>
</header>

<section class="tiles">
  <div class="tile"><div class="label">markets</div><div class="value" id="markets">—</div><div class="sub"><span id="tokens">—</span> tokens · <span id="shard-count">—</span> shards</div></div>
  <div class="tile" id="tile-msgs"><div class="label">msg/s now</div><div class="value" id="msgrate">—</div><div class="sub">1m: <span id="msgrate-1m">—</span> · 10m: <span id="msgrate-10m">—</span></div></div>
  <div class="tile" id="tile-queue"><div class="label">queue</div><div class="value" id="queue">—</div><div class="sub">max: <span id="queue-max">—</span></div></div>
  <div class="tile" id="tile-drops"><div class="label">total drops</div><div class="value" id="drops">—</div></div>
  <div class="tile" id="tile-alive"><div class="label">alive shards</div><div class="value" id="shards-alive">—</div><div class="sub">/ <span id="shards-total">—</span></div></div>
  <div class="tile" id="tile-quar"><div class="label">quarantined</div><div class="value" id="quarantined">0</div></div>
  <div class="tile"><div class="label">total sessions</div><div class="value" id="sessions">—</div><div class="sub">reconnects: <span id="reconnects">—</span></div></div>
  <div class="tile"><div class="label">total messages</div><div class="value" id="total-msgs">—</div></div>
</section>

<section class="charts">
  <div class="chart-box"><h3>msg/sec (1-min buckets)</h3><canvas id="chart-msgs"></canvas></div>
  <div class="chart-box"><h3>drops/sec</h3><canvas id="chart-drops"></canvas></div>
  <div class="chart-box"><h3>queue depth (max in bucket)</h3><canvas id="chart-queue"></canvas></div>
  <div class="chart-box"><h3>total sessions (reconnects)</h3><canvas id="chart-sessions"></canvas></div>
</section>

<section>
  <h2>Shards</h2>
  <div class="panel"><table id="shards-table">
    <thead><tr>
      <th>id</th><th>state</th><th>markets</th><th>sessions</th><th>msgs</th><th>last msg age</th><th>last error</th>
    </tr></thead>
    <tbody></tbody>
  </table></div>
</section>

<section>
  <h2>Events (most recent 200)</h2>
  <div class="panel"><ul class="events" id="events-list"></ul></div>
</section>

</div><!-- /#view-status -->

<div id="view-markets" class="hidden">
  <aside class="sidebar">
    <h3>Categories</h3>
    <ul id="categories-list"><li class="empty">loading…</li></ul>
  </aside>
  <main class="grid-wrap">
    <div class="grid-header">
      <h2 id="grid-title">Markets</h2>
      <span class="grid-meta" id="grid-meta">—</span>
    </div>
    <div class="markets-panel">
      <table id="markets-table">
        <thead><tr>
          <th>Question</th>
          <th class="num">YES</th>
          <th class="num">NO</th>
          <th class="num">spread</th>
          <th class="num">vol 24h</th>
          <th class="num">last upd</th>
        </tr></thead>
        <tbody><tr><td colspan="6" class="empty">pick a category</td></tr></tbody>
      </table>
    </div>
  </main>
</div><!-- /#view-markets -->

<script>
const fmt = (n) => n == null ? "—" : (typeof n === 'number' ? n.toLocaleString() : n);
function fmtDuration(s) {
  if (s == null) return "—";
  s = Math.floor(s);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  if (h > 0) return `${h}h ${String(m).padStart(2,'0')}m ${String(sec).padStart(2,'0')}s`;
  if (m > 0) return `${m}m ${String(sec).padStart(2,'0')}s`;
  return `${sec}s`;
}
function fmtAge(s) {
  if (s == null) return "—";
  if (s < 60) return `${s.toFixed(1)}s`;
  if (s < 3600) return `${(s/60).toFixed(1)}m`;
  return `${(s/3600).toFixed(2)}h`;
}

const CHART_DEFAULTS = {
  responsive: true, maintainAspectRatio: false, animation: false,
  scales: {
    x: { ticks: { color: '#8b96a5', maxTicksLimit: 8 }, grid: { color: '#1b2330' } },
    y: { ticks: { color: '#8b96a5' }, grid: { color: '#1b2330' }, beginAtZero: true },
  },
  plugins: { legend: { display: false } },
  elements: { point: { radius: 0 }, line: { borderWidth: 1.5, tension: 0.2 } },
};

function makeChart(canvasId, color) {
  const ctx = document.getElementById(canvasId).getContext('2d');
  return new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [{ data: [], borderColor: color, backgroundColor: color + '22', fill: true }] },
    options: CHART_DEFAULTS,
  });
}

let chartMsgs, chartDrops, chartQueue, chartSessions;

async function pollStatus() {
  try {
    const r = await fetch('/api/status'); if (!r.ok) return;
    const d = await r.json();
    const p = d.probe || {}, a = d.aggregate || {}, shards = d.shards || [];

    document.getElementById('elapsed').textContent = fmtDuration(p.elapsed_s);
    if (p.target_duration_s) {
      const pct = ((p.elapsed_s || 0) / p.target_duration_s * 100).toFixed(1);
      document.getElementById('elapsed-pct').textContent = ` / ${fmtDuration(p.target_duration_s)} (${pct}%)`;
    }
    document.getElementById('start-time').textContent = p.start_wall_utc || "—";
    document.getElementById('target-duration').textContent = fmtDuration(p.target_duration_s);
    document.getElementById('shard-size').textContent = p.shard_size || "—";
    document.getElementById('last-updated').textContent = (p.wall_utc || "").replace('T',' ').slice(0,19);

    if (p.fatal_error) {
      document.getElementById('fatal-wrap').classList.remove('hidden');
      document.getElementById('fatal-msg').textContent = p.fatal_error;
    }

    document.getElementById('markets').textContent = fmt(a.market_count);
    document.getElementById('tokens').textContent = fmt(a.token_count);
    document.getElementById('shard-count').textContent = fmt(a.shard_count);
    document.getElementById('msgrate').textContent = fmt(a.msg_per_sec_1min);
    document.getElementById('msgrate-1m').textContent = fmt(a.msg_per_sec_1min);
    document.getElementById('msgrate-10m').textContent = fmt(a.msg_per_sec_10min);
    document.getElementById('queue').textContent = fmt(a.queue_depth);
    document.getElementById('queue-max').textContent = fmt(a.queue_maxsize);
    document.getElementById('drops').textContent = fmt(a.drops);
    document.getElementById('shards-alive').textContent = fmt(a.alive_shards);
    document.getElementById('shards-total').textContent = fmt(a.shard_count);
    document.getElementById('quarantined').textContent = fmt(a.quarantined_shards);
    document.getElementById('sessions').textContent = fmt(a.total_sessions);
    document.getElementById('reconnects').textContent = fmt((a.total_sessions || 0) - (a.shard_count || 0));
    document.getElementById('total-msgs').textContent = fmt(a.total_messages);

    // Tile coloring
    document.getElementById('tile-quar').classList.toggle('warn', a.quarantined_shards > 0);
    document.getElementById('tile-alive').classList.toggle('danger', a.alive_shards < a.shard_count);
    document.getElementById('tile-alive').classList.toggle('good', a.alive_shards === a.shard_count);
    const qpct = (a.queue_depth || 0) / Math.max(1, a.queue_maxsize || 1);
    document.getElementById('tile-queue').classList.toggle('warn', qpct > 0.5);
    document.getElementById('tile-queue').classList.toggle('danger', qpct > 0.9);

    // Shards table — retain dead shards visibly
    const tbody = document.querySelector('#shards-table tbody');
    tbody.innerHTML = shards.map(s => {
      let cls = '';
      if (s.state === 'connected') cls = 'shard-conn';
      else if (s.state === 'quarantined') cls = 'shard-quar';
      else if (s.state === 'reconnecting' || s.state === 'connecting') cls = 'shard-recon';
      else if (s.state === 'stopped' || !s.first_msg_received) cls = 'shard-dead';
      const err = s.last_disconnect_reason || s.last_open_failure || '';
      return `<tr class="${cls}">
        <td>#${s.shard_id}</td>
        <td class="state">${s.state}</td>
        <td>${s.markets}</td>
        <td>${s.session_count}</td>
        <td>${fmt(s.message_count)}</td>
        <td>${fmtAge(s.last_msg_age_s)}</td>
        <td class="err" title="${err.replace(/"/g,'&quot;')}">${err}</td>
      </tr>`;
    }).join('');
  } catch (e) { console.warn('status poll failed', e); }
}

async function pollHistory() {
  try {
    const r = await fetch('/api/history?bucket_s=60'); if (!r.ok) return;
    const d = await r.json();
    const buckets = d.buckets || [];
    const labels = buckets.map(b => fmtDuration(b.t));
    const apply = (chart, field) => {
      chart.data.labels = labels;
      chart.data.datasets[0].data = buckets.map(b => b[field]);
      chart.update('none');
    };
    apply(chartMsgs, 'msgs_per_sec');
    apply(chartDrops, 'drops_per_sec');
    apply(chartQueue, 'queue_depth');
    apply(chartSessions, 'total_sessions');
  } catch (e) { console.warn('history poll failed', e); }
}

async function pollEvents() {
  try {
    const r = await fetch('/api/events?limit=200'); if (!r.ok) return;
    const d = await r.json();
    const evs = (d.events || []).slice().reverse();
    const ul = document.getElementById('events-list');
    ul.innerHTML = evs.map(e => {
      const isErr = e.kind === 'state_change' && (e.to_state === 'quarantined' || e.to_state === 'reconnecting');
      let detail = '';
      if (e.kind === 'state_change') detail = `${e.from_state} → ${e.to_state}`;
      else if (e.kind === 'session_started') detail = `session #${e.session_count}`;
      else if (e.kind === 'init') detail = `state=${e.state}`;
      const reason = e.reason ? ` · ${e.reason}` : '';
      return `<li class="${isErr ? 'danger' : ''}">
        <span class="t">${fmtDuration(e.t)}</span>
        <span class="sid">#${e.shard_id}</span>
        <span class="kind ${e.kind}">${e.kind}</span>
        <span class="reason">${detail}${reason}</span>
      </li>`;
    }).join('');
  } catch (e) { console.warn('events poll failed', e); }
}

// ---------- Hash router + Markets browse view ----------

const VIEW_STATUS = 'status';
const VIEW_MARKETS = 'markets';
let currentView = VIEW_STATUS;
let currentCat = null;       // active tag slug, or null
let categoriesCache = [];    // last categories payload
let gridSort = 'volume';
let gridPollTimer = null;

function parseHash() {
  const h = (location.hash || '').replace(/^#/, '');
  if (!h || h === VIEW_STATUS) return { view: VIEW_STATUS, cat: null };
  if (h === VIEW_MARKETS) return { view: VIEW_MARKETS, cat: null };
  if (h.startsWith('cat/')) return { view: VIEW_MARKETS, cat: decodeURIComponent(h.slice(4)) };
  // Unknown hash → fall back to status, don't blow up.
  return { view: VIEW_STATUS, cat: null };
}

function applyRoute() {
  const { view, cat } = parseHash();
  currentView = view;

  document.getElementById('view-status').classList.toggle('hidden', view !== VIEW_STATUS);
  document.getElementById('view-markets').classList.toggle('hidden', view !== VIEW_MARKETS);
  document.getElementById('tab-status').classList.toggle('active', view === VIEW_STATUS);
  document.getElementById('tab-markets').classList.toggle('active', view === VIEW_MARKETS);

  if (view === VIEW_MARKETS) {
    // First entry to markets view → load categories so the sidebar paints.
    if (categoriesCache.length === 0) loadCategories();
    pickCategory(cat, /*pushHash=*/false);
    startGridPolling();
  } else {
    stopGridPolling();
  }
}

async function loadCategories() {
  try {
    const r = await fetch('/api/categories');
    if (!r.ok) return;
    const d = await r.json();
    categoriesCache = d.categories || [];
    renderCategories();
    // If we landed on #markets with no slug, auto-pick highest-count category
    // once categories arrive. Same for an unknown slug that didn't match.
    if (currentView === VIEW_MARKETS && !currentCat && categoriesCache.length > 0) {
      pickCategory(categoriesCache[0].slug, /*pushHash=*/true);
    }
  } catch (e) { console.warn('categories load failed', e); }
}

function renderCategories() {
  const ul = document.getElementById('categories-list');
  if (categoriesCache.length === 0) {
    ul.innerHTML = '<li class="empty">no categories yet</li>';
    return;
  }
  ul.innerHTML = categoriesCache.map(c => {
    const active = (currentCat === c.slug) ? ' active' : '';
    const slug = encodeURIComponent(c.slug);
    return `<li><a href="#cat/${slug}" class="${active.trim()}" data-slug="${c.slug}">
              <span>${escapeHtml(c.label)}</span>
              <span class="count">${c.count}</span>
            </a></li>`;
  }).join('');
}

function pickCategory(slug, pushHash) {
  // If no slug requested but a default is available, pick the first (highest count).
  if (!slug && categoriesCache.length > 0) slug = categoriesCache[0].slug;
  currentCat = slug || null;

  if (pushHash && slug) {
    location.hash = '#cat/' + encodeURIComponent(slug);
    return; // applyRoute will fire from hashchange and re-enter here
  }
  renderCategories();

  if (!currentCat) {
    document.getElementById('grid-title').textContent = 'Markets';
    document.getElementById('grid-meta').textContent = '—';
    document.querySelector('#markets-table tbody').innerHTML =
      '<tr><td colspan="6" class="empty">pick a category</td></tr>';
    return;
  }
  const cat = categoriesCache.find(c => c.slug === currentCat);
  document.getElementById('grid-title').textContent = cat ? cat.label : currentCat;
  loadMarkets();
}

function startGridPolling() {
  stopGridPolling();
  if (currentCat) gridPollTimer = setInterval(loadMarkets, 3000);
}
function stopGridPolling() {
  if (gridPollTimer) { clearInterval(gridPollTimer); gridPollTimer = null; }
}

async function loadMarkets() {
  if (!currentCat) return;
  try {
    const url = `/api/markets?tag=${encodeURIComponent(currentCat)}&sort=${gridSort}&limit=100`;
    const r = await fetch(url);
    if (!r.ok) return;
    const d = await r.json();
    renderMarkets(d.markets || [], d.count || 0);
  } catch (e) { console.warn('markets load failed', e); }
}

function renderMarkets(rows, total) {
  const tbody = document.querySelector('#markets-table tbody');
  const meta = document.getElementById('grid-meta');
  meta.textContent = `${total} market${total === 1 ? '' : 's'} · sorted by ${gridSort}`;
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty">no markets in this category yet</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(r => {
    const stale = (r.last_msg_age_s != null && r.last_msg_age_s > 60) ? ' stale' : '';
    const yes = fmtPx(r.yes_bid, r.yes_ask);
    const no  = fmtPx(r.no_bid, r.no_ask);
    const spread = r.spread != null ? r.spread.toFixed(3) : '<span class="nobook">—</span>';
    const vol = fmtMoney(r.volume_24h);
    const age = r.last_msg_age_s == null ? '<span class="nobook">—</span>' : fmtAge(r.last_msg_age_s);
    return `<tr class="${stale.trim()}">
      <td class="q" title="${escapeAttr(r.question)}">${escapeHtml(r.question)}</td>
      <td class="num">${yes}</td>
      <td class="num">${no}</td>
      <td class="num">${spread}</td>
      <td class="num">${vol}</td>
      <td class="num">${age}</td>
    </tr>`;
  }).join('');
}

function fmtPx(bid, ask) {
  // Display the side that has data; mid if both exist.
  if (bid == null && ask == null) return '<span class="nobook">—</span>';
  if (bid == null) return ask.toFixed(3);
  if (ask == null) return bid.toFixed(3);
  return ((bid + ask) / 2).toFixed(3);
}
function fmtMoney(v) {
  if (v == null || v === 0) return '<span class="nobook">—</span>';
  if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
  if (v >= 1e3) return '$' + (v / 1e3).toFixed(1) + 'K';
  return '$' + v.toFixed(0);
}
function escapeHtml(s) {
  if (s == null) return '';
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
function escapeAttr(s) { return escapeHtml(s).replace(/"/g, '&quot;'); }

window.addEventListener('hashchange', applyRoute);

window.addEventListener('load', () => {
  chartMsgs = makeChart('chart-msgs', '#2dd4bf');
  chartDrops = makeChart('chart-drops', '#ef4444');
  chartQueue = makeChart('chart-queue', '#fbbf24');
  chartSessions = makeChart('chart-sessions', '#a78bfa');
  pollStatus(); pollHistory(); pollEvents();
  setInterval(pollStatus, 2000);
  setInterval(pollHistory, 30000);
  setInterval(pollEvents, 5000);

  // Markets browse view: refresh categories every 60s; route on initial load.
  setInterval(() => { if (currentView === VIEW_MARKETS) loadCategories(); }, 60000);
  applyRoute();
});
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


def create_app(recorder: HistoryRecorder, ws: PolymarketUniversalWS) -> FastAPI:
    app = FastAPI(title="Universal WS Soak Dashboard")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return DASHBOARD_HTML

    @app.get("/api/status")
    async def status():
        return JSONResponse(recorder.snapshot())

    @app.get("/api/history")
    async def history(bucket_s: int = 60):
        return JSONResponse({"buckets": recorder.history(bucket_s=bucket_s)})

    @app.get("/api/events")
    async def events(limit: int = 500):
        evs = list(recorder.events)[-limit:]
        return JSONResponse({"events": evs})

    @app.get("/api/categories")
    async def categories():
        return JSONResponse({"categories": ws.categories()})

    @app.get("/api/markets")
    async def markets(tag: str, sort: str = "volume", limit: int = 100):
        # Clamp limit to a sensible ceiling so a hostile/typoed query can't
        # try to materialize the entire universe in one response.
        limit = max(1, min(int(limit), 500))
        rows = ws.markets_by_tag(tag_slug=tag, sort=sort, limit=limit)
        return JSONResponse({"tag": tag, "sort": sort, "count": len(rows), "markets": rows})

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main_async(args) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ws = PolymarketUniversalWS(
        shard_size=args.shard_size,
        max_markets=args.max_markets,
    )
    recorder = HistoryRecorder(ws, period_s=args.sample_period)
    recorder.set_probe_meta(
        start_wall_utc=utcnow_iso(),
        target_duration_s=args.duration,
        shard_size=args.shard_size,
        max_markets=args.max_markets,
        fatal_error=None,
    )

    app = create_app(recorder, ws)
    config = uvicorn.Config(app, host="0.0.0.0", port=args.port, log_level="warning")
    server = uvicorn.Server(config)

    stop = asyncio.Event()
    consumer_counter = [0]
    err: Optional[str] = None
    start_mono = time.monotonic()
    server_task: Optional[asyncio.Task] = None
    consumer_task: Optional[asyncio.Task] = None

    async def consumer() -> None:
        try:
            async for _mid, _book in ws.iter_updates():
                consumer_counter[0] += 1
                if stop.is_set():
                    return
        except asyncio.CancelledError:
            raise

    try:
        await ws.start(market_ids=None)
        await recorder.start()
        server_task = asyncio.create_task(server.serve(), name="uvicorn")
        consumer_task = asyncio.create_task(consumer(), name="consumer")
        logger.info(f"Dashboard live at http://localhost:{args.port}/ — running for {args.duration}s")

        try:
            await asyncio.wait_for(stop.wait(), timeout=args.duration)
        except asyncio.TimeoutError:
            pass
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        logger.error(f"FATAL during probe: {err}")
        recorder.set_probe_meta(fatal_error=err)
    finally:
        stop.set()
        if server is not None:
            server.should_exit = True
        for t in (consumer_task, server_task):
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        await recorder.stop()
        await ws.stop()

    elapsed = time.monotonic() - start_mono
    end_status = ws.status()
    shards = end_status["shards"]
    summary = {
        "probe": {
            "type": "universal_ws_dashboard",
            "duration_target_s": args.duration,
            "duration_actual_s": round(elapsed, 2),
            "start_wall_utc": recorder.probe_meta.get("start_wall_utc"),
            "end_wall_utc": utcnow_iso(),
            "shard_size": args.shard_size,
            "max_markets": args.max_markets,
            "fatal_error": err,
        },
        "aggregate": {
            "shard_count": end_status["shard_count"],
            "market_count": end_status["market_count"],
            "token_count": end_status["token_count"],
            "total_messages": sum(s["message_count"] for s in shards),
            "total_bytes": sum(s["bytes_received"] for s in shards),
            "msg_per_sec": round(sum(s["message_count"] for s in shards) / elapsed, 1) if elapsed else 0,
            "total_sessions": sum(s["session_count"] for s in shards),
            "shards_with_first_msg": sum(1 for s in shards if s["first_msg_received"]),
            "shards_quarantined": sum(1 for s in shards if s["state"] == "quarantined"),
            "drops": end_status["drops"],
            "iter_updates_yielded": consumer_counter[0],
            "queue_depth_final": end_status["queue_depth"],
            "queue_maxsize": end_status["queue_maxsize"],
        },
        "shards": shards,
        "events": list(recorder.events),
        "history_60s_buckets": recorder.history(bucket_s=60),
    }
    with open(args.output, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Wrote {args.output} (duration={elapsed:.0f}s)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Universal WS soak probe with live dashboard")
    parser.add_argument("--duration", type=int, required=True, help="Probe duration in seconds (86400 for 24h)")
    parser.add_argument("--output", type=str, required=True, help="Final summary JSON path")
    parser.add_argument("--port", type=int, default=8889, help="Dashboard HTTP port")
    parser.add_argument("--shard-size", type=int, default=100, help="Markets per WS shard")
    parser.add_argument("--max-markets", type=int, default=5000, help="Universe cap")
    parser.add_argument("--sample-period", type=float, default=2.0, help="Status sampling period (s)")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
