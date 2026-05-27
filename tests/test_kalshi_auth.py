"""
Tests for Kalshi authentication primitives (kalshi_client/auth.py).

The module under test does not exist yet — these tests are written first
per TDD discipline (tasks/kalshi-universal-ws.md Phase 0).
"""

import base64
import time

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa


@pytest.fixture(scope="module")
def rsa_keypair():
    """Throwaway RSA-2048 keypair for signing/verification tests."""
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return priv, priv.public_key()


def _verify(public_key, signature_b64: str, message: bytes) -> None:
    """Verify an RSA-PSS+SHA-256 signature; raises InvalidSignature on failure."""
    sig = base64.b64decode(signature_b64)
    public_key.verify(
        sig,
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
        hashes.SHA256(),
    )


def test_sign_produces_valid_rsa_pss_signature(rsa_keypair):
    """sign() must produce a base64 RSA-PSS+SHA-256 signature that verifies."""
    from kalshi_client.auth import sign

    priv, pub = rsa_keypair
    timestamp_ms = 1727000000000
    method = "GET"
    path = "/trade-api/ws/v2"

    signature_b64 = sign(timestamp_ms, method, path, priv)

    expected_msg = f"{timestamp_ms}{method}{path}".encode("utf-8")
    _verify(pub, signature_b64, expected_msg)


def test_load_private_key_round_trips_a_pem_string(rsa_keypair):
    """load_private_key() parses a PEM and produces a key usable for signing."""
    from cryptography.hazmat.primitives import serialization

    from kalshi_client.auth import load_private_key, sign

    priv, pub = rsa_keypair
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")

    loaded = load_private_key(pem)
    signature_b64 = sign(1727000000000, "GET", "/trade-api/ws/v2", loaded)

    _verify(pub, signature_b64, b"1727000000000GET/trade-api/ws/v2")


def test_load_private_key_rejects_invalid_pem():
    """load_private_key() raises a ValueError on garbage input."""
    from kalshi_client.auth import load_private_key

    with pytest.raises(ValueError):
        load_private_key("this is not a PEM key")


def test_build_headers_returns_three_required_kalshi_headers(rsa_keypair):
    """build_headers() must produce KEY, SIGNATURE, TIMESTAMP headers with a valid sig."""
    from kalshi_client.auth import build_headers

    priv, pub = rsa_keypair
    api_key_id = "test-key-id-1234"
    method = "GET"
    path = "/trade-api/ws/v2"

    headers = build_headers(api_key_id, priv, method, path)

    assert set(headers) == {
        "KALSHI-ACCESS-KEY",
        "KALSHI-ACCESS-SIGNATURE",
        "KALSHI-ACCESS-TIMESTAMP",
    }
    assert headers["KALSHI-ACCESS-KEY"] == api_key_id

    timestamp_ms = int(headers["KALSHI-ACCESS-TIMESTAMP"])
    expected_msg = f"{timestamp_ms}{method}{path}".encode("utf-8")
    _verify(pub, headers["KALSHI-ACCESS-SIGNATURE"], expected_msg)
