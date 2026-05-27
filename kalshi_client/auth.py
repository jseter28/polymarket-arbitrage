"""
Kalshi authentication primitives.

Kalshi uses RSA-PSS-signed handshake headers for both REST and WebSocket
authentication. The signing string is the concatenation:

    timestamp_ms + method + path

(No separator characters.) The signature is RSA-PSS with MGF1(SHA-256),
salt length = 32 bytes, base64-encoded.

References:
    https://docs.kalshi.com/getting_started/quick_start_websockets
"""

import base64
import time

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


def load_private_key(pem: str) -> RSAPrivateKey:
    """Parse an RSA private key from a PEM string. Raises ValueError on invalid input."""
    return serialization.load_pem_private_key(pem.encode("utf-8"), password=None)


def sign(timestamp_ms: int, method: str, path: str, private_key: RSAPrivateKey) -> str:
    """Sign a Kalshi auth payload and return the base64 signature."""
    message = f"{timestamp_ms}{method}{path}".encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("ascii")


def build_headers(
    api_key_id: str,
    private_key: RSAPrivateKey,
    method: str,
    path: str,
) -> dict[str, str]:
    """Build the three signed headers Kalshi requires on every authed request."""
    timestamp_ms = int(time.time() * 1000)
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": sign(timestamp_ms, method, path, private_key),
        "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
    }
