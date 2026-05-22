# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install (Python 3.10+)
pip install -r requirements.txt

# Run bot + dashboard (primary entry point)
python run_with_dashboard.py                    # dry-run, dashboard on http://localhost:8888
python run_with_dashboard.py --live             # live trading
python run_with_dashboard.py --port 8080        # custom dashboard port
python run_with_dashboard.py -c config.live.yaml

# Run bot only (no dashboard)
python main.py [--live | --dry-run] [-c config.yaml] [-v]
python main.py --backtest --backtest-duration 300

# Sanity checks before going live
python test_connection.py -c config.live.yaml   # verifies API creds + Gamma reachability
python test_real_data.py                        # fetches a live order book end-to-end

# Tests
pytest tests/ -v
pytest tests/test_arb_engine.py -v
pytest tests/test_arb_engine.py::TestClassName::test_method -v
pytest tests/ --cov=core --cov=polymarket_client

# Formatting / type-checking (dev deps shipped in requirements.txt)
black .
mypy .
```

Note: The README references `http://localhost:8000`, but `run_with_dashboard.py` defaults to **8888**. Use `--port 8000` to match the README, or just open the port the bot logs at startup.

## Architecture

The bot is an async (`asyncio`) trading engine that monitors prediction markets on **Polymarket** and **Kalshi** and emits opportunities through a single `Opportunity` signal pipeline.

### Two entry points, one engine

- `main.py` → `TradingBot`: Polymarket-only, headless. Wires `PolymarketClient → DataFeed → ArbEngine → ExecutionEngine`.
- `run_with_dashboard.py` → `TradingBotWithDashboard`: superset of the above. Additionally instantiates `KalshiClient`, `MarketMatcher`, and `CrossPlatformArbEngine`, plus a FastAPI server in `dashboard/server.py` running in a background thread.

Both wire the same five `core/` components — diverge only in which clients/engines they additionally start.

### Component graph

```
PolymarketClient  ─┐                              ┌─► ArbEngine (single-platform: bundle arb, MM)
                   ├─► DataFeed (orderbooks +  ───┤
KalshiClient ──────┘    positions, in-memory)     └─► CrossPlatformArbEngine (Polymarket↔Kalshi)
                                                          │
                                                          ▼
                                          Opportunity → ExecutionEngine
                                                          │
                                          RiskManager ◄───┤───► Portfolio
                                          (gates orders)        (positions + PnL)
```

- **`DataFeed`** (`core/data_feed.py`) — owns in-memory `MarketState`. Streams order books via WebSocket and polls positions via REST. Calls `on_update(market_id, MarketState)` on every change; the bot wires this to `ArbEngine.analyze()`.
- **`ArbEngine`** (`core/arb_engine.py`) — detects single-platform opportunities: bundle arb (YES+NO ≠ $1) and market-making (when spread ≥ `min_spread`). Returns `Opportunity` objects keyed by `OpportunityType`.
- **`CrossPlatformArbEngine`** (`core/cross_platform_arb.py`) — matches Polymarket questions to Kalshi tickers via text similarity (`MarketMatcher`, threshold `min_match_similarity`), then watches matched pairs for price divergence. Has its own NFL team / keyword normalization.
- **`ExecutionEngine`** (`core/execution.py`) — receives `Opportunity` signals, applies slippage tolerance, places orders via the client. All orders pass through `RiskManager.check_*` first.
- **`RiskManager`** (`core/risk_manager.py`) — enforces per-market and global exposure caps, daily-loss stop, kill switch. Reads PnL via `update_pnl()` in the monitoring loop.
- **`Portfolio`** (`core/portfolio.py`) — tracks positions and realized/unrealized PnL.

### Trading modes (two orthogonal axes)

Defined in `config.yaml` under `mode:`:

| Axis | Values | Where it branches |
|---|---|---|
| `trading_mode` | `dry_run` \| `live` | `BotConfig.is_dry_run` → `PolymarketClient(dry_run=...)`, `ExecutionConfig(dry_run=...)`. In dry-run, `_simulate_fills()` task randomly fills open orders at `fill_probability`. |
| `data_mode` | `real` \| `simulation` | Consumed inside `DataFeed` / `PolymarketClient` to choose between live Gamma+CLOB feeds and synthetic order books that periodically inject mispricings (for screenshots/demos). |

These are independent: you can `live` + `simulation` (don't), or `dry_run` + `real` (the common dev mode).

### Dashboard wiring

`dashboard/server.py` is a single ~2,400-line file holding the FastAPI app, embedded HTML/JS, and a module-level `dashboard_state` object. `dashboard/integration.py` defines `DashboardIntegration`, which the bot pushes updates into (opportunities found, orders placed, PnL snapshots). The FastAPI server runs in a `threading.Thread` from `run_with_dashboard.py`; the bot itself stays on the main asyncio loop. Don't `await` dashboard calls from the bot — they're sync push, not coroutine.

### Config flow

`utils/config_loader.load_config(path)` → validated `BotConfig` dataclass tree (`api`, `trading`, `risk`, `mode`, `logging`, `monitoring`). `validate_config()` (called from `load_config`) hard-fails when `trading_mode: live` and `api_key`/`private_key` are still the placeholder strings — this is the guardrail against accidental live trading with a half-edited config. Sensitive overrides go in `config.live.yaml` (gitignored).

## Conventions worth knowing

- **All trading code is async.** Components expose `await start()` / `await stop()`; never block the loop. Use `asyncio.create_task` for background work (see `_monitoring_loop`, `_simulate_fills` in `main.py`).
- **`Opportunity` is the only signal type** that crosses the detect→execute boundary. Both `ArbEngine` and `CrossPlatformArbEngine` produce them; `ExecutionEngine.submit_signal()` is the single consumer. New strategies should emit `Opportunity` rather than introduce a parallel signal channel.
- **Edge math always includes fees + gas.** When changing arb detection, use the same `maker_fee_bps` / `taker_fee_bps` / `estimated_gas_per_order` pulled from config — do not re-derive net edge inline.
- **Polymarket markets are referenced by two IDs**: the Gamma `market_id` (used for discovery / order book lookup) and the per-token `yes_token_id` / `no_token_id` (used at the CLOB layer). `test_real_data.py` shows the round-trip; preserve both when adding handling for new markets.
- **Kalshi public data needs no auth** (see `kalshi_client/api.py`); only Polymarket trading requires `api_key` + `private_key`. Don't add Kalshi auth flows speculatively.
- **Dry-run logs and trades look real.** Always check `config.mode.trading_mode` / startup banner before assuming a session is paper or live.
