#!/usr/bin/env python3
"""
Test Kalshi Authenticated REST Connection
==========================================

End-to-end smoke test of the auth signing chain. Loads kalshi_api_key and
kalshi_private_key from a config file, signs a request to /portfolio/balance
(read-only authenticated endpoint), and reports whether the signature was
accepted.

This proves the auth primitives in `kalshi_client/auth.py` produce a signature
Kalshi accepts — Phase 0 gate before layering WebSocket on top.

Defaults to the demo host because the Phase 0 credentials currently in use
are demo creds. To verify against production, pass `--base-url
https://api.elections.kalshi.com/trade-api/v2` once you have prod credentials.

Usage:
    python3 test_kalshi_connection.py
    python3 test_kalshi_connection.py --config config.live.yaml
    python3 test_kalshi_connection.py --base-url https://api.elections.kalshi.com/trade-api/v2
"""

import argparse
import asyncio
import sys

import httpx

from kalshi_client.auth import build_headers, load_private_key
from utils.config_loader import load_config

DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"


async def test_kalshi_connection(
    config_path: str = "config.live.yaml",
    base_url_override: str | None = None,
) -> bool:
    print("=" * 60)
    print("🔌 Kalshi Authenticated REST Connection Test")
    print("=" * 60)

    try:
        config = load_config(config_path)
        print(f"✅ Config loaded from {config_path}")
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        return False

    api_key_id = config.api.kalshi_api_key
    private_key_pem = config.api.kalshi_private_key
    base_url = base_url_override or DEMO_BASE_URL

    if not api_key_id or api_key_id.startswith("REPLACE_"):
        print("❌ kalshi_api_key not configured in config")
        return False
    if not private_key_pem or "BEGIN" not in private_key_pem:
        print("❌ kalshi_private_key not configured in config (PEM expected)")
        return False

    print(f"   Base URL: {base_url}")
    print(f"   Key ID:   {api_key_id[:8]}…")

    try:
        private_key = load_private_key(private_key_pem)
        print("✅ Private key parsed")
    except Exception as e:
        print(f"❌ Failed to parse private key: {e}")
        return False

    path = "/portfolio/balance"
    full_path = "/trade-api/v2" + path

    print()
    print(f"📡 Signing request: GET {full_path}")
    headers = build_headers(api_key_id, private_key, "GET", full_path)
    headers["Accept"] = "application/json"

    print(f"   Timestamp: {headers['KALSHI-ACCESS-TIMESTAMP']}")
    print(
        f"   Signature: {headers['KALSHI-ACCESS-SIGNATURE'][:32]}… ({len(headers['KALSHI-ACCESS-SIGNATURE'])} chars)"
    )

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(f"{base_url}{path}", headers=headers)
        except Exception as e:
            print(f"❌ HTTP request failed: {e}")
            return False

    print()
    print(f"   Status: HTTP {resp.status_code}")

    if resp.status_code == 200:
        data = resp.json()
        print("✅ Auth accepted by Kalshi")
        if "balance" in data:
            print(f"   Balance (raw fields): {sorted(data.keys())}")
        print()
        print("=" * 60)
        print("✅ Kalshi authentication chain VERIFIED end-to-end")
        print("=" * 60)
        return True

    if resp.status_code in (401, 403):
        print(f"❌ Auth rejected: HTTP {resp.status_code}")
        print(f"   Body: {resp.text[:300]}")
        print()
        print("Signing chain produced a signature Kalshi did not accept. Check:")
        print("  - api_key_id matches the public key registered with Kalshi")
        print("  - private_key PEM is the partner of the registered public key")
        print("  - System clock is within ~5s of UTC")
        return False

    print(f"⚠️  Unexpected status {resp.status_code}")
    print(f"   Body: {resp.text[:300]}")
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Test Kalshi authenticated connection")
    parser.add_argument(
        "-c", "--config", default="config.live.yaml", help="Config file"
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help=f"REST base URL (default: {DEMO_BASE_URL})",
    )
    args = parser.parse_args()

    success = asyncio.run(test_kalshi_connection(args.config, args.base_url))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
