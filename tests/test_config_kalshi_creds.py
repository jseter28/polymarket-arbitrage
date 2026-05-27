"""
Tests that BotConfig surfaces Kalshi credentials from YAML.

Until Phase 0 of kalshi-universal-ws, ApiConfig had no kalshi_api_key /
kalshi_private_key fields and _build_dataclass silently dropped them.
"""

import textwrap
from pathlib import Path

from utils.config_loader import load_config


def test_load_config_exposes_kalshi_api_key_and_private_key(tmp_path: Path) -> None:
    yaml_body = textwrap.dedent("""
        api:
          kalshi_api_key: "test-key-id-abc123"
          kalshi_private_key: "-----BEGIN RSA PRIVATE KEY-----\\nFAKEPEM\\n-----END RSA PRIVATE KEY-----"
        mode:
          trading_mode: dry_run
        """)
    cfg_path = tmp_path / "test_config.yaml"
    cfg_path.write_text(yaml_body)

    config = load_config(str(cfg_path))

    assert config.api.kalshi_api_key == "test-key-id-abc123"
    assert "BEGIN RSA PRIVATE KEY" in config.api.kalshi_private_key
