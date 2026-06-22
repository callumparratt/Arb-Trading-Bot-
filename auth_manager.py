#!/usr/bin/env python3
"""
auth_manager.py
===============

Cryptographic authentication layer for live-ready Kalshi + Polymarket access.

Responsibilities
----------------
1. Load API credentials *only* from system environment variables. Nothing is
   hardcoded, no `.env` file is read, and secret values are never logged.
       KALSHI_API_KEY_ID        - Kalshi access key id (public identifier)
       KALSHI_PRIVATE_KEY_PATH  - filesystem path to the Kalshi RSA private key (PEM)
       POLY_PRIVATE_KEY         - Polymarket signer EOA private key (hex)
       POLY_CLOB_API_KEY        - Polymarket CLOB API key (L2 identifier)

2. Kalshi v2 request signing: RSA-PSS (SHA-256, MGF1, digest-length salt) over
   the exact string `timestamp_ms + METHOD + path`, base64-encoded.

3. Polymarket EIP-712 (L1) header boilerplate via `eth_account`, ready to be
   handed to the CLOB client.

This module PREPARES authenticated headers; it does NOT send requests or place
orders. Wiring it to an order endpoint is an explicit, separate step.

Security notes
--------------
  * Treat KALSHI_PRIVATE_KEY_PATH and POLY_PRIVATE_KEY as secrets. Keep the key
    file at 0600 perms and the env var out of shell history / logs.
  * This file deliberately has no "place order" path. Keep signing and
    execution separated so a bug here can't move funds.

Install the optional crypto deps when you go live:
    pip install cryptography eth-account
"""

import base64
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

# --- optional heavy deps: import lazily with friendly errors -----------------
try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    _HAS_CRYPTOGRAPHY = True
    _CRYPTOGRAPHY_IMPORT_ERR = ""
except ImportError as _exc:  # pragma: no cover - environment dependent
    _HAS_CRYPTOGRAPHY = False
    _CRYPTOGRAPHY_IMPORT_ERR = str(_exc)

try:
    from eth_account import Account
    from eth_account.messages import encode_typed_data
    _HAS_ETH_ACCOUNT = True
    _ETH_ACCOUNT_IMPORT_ERR = ""
except ImportError as _exc:  # pragma: no cover - environment dependent
    _HAS_ETH_ACCOUNT = False
    _ETH_ACCOUNT_IMPORT_ERR = str(_exc)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class AuthError(Exception):
    """Base class for all authentication-layer failures."""


class MissingCredentialsError(AuthError):
    """One or more required environment variables were not set."""

    def __init__(self, missing: List[str]) -> None:
        self.missing = list(missing)
        joined = ", ".join(self.missing)
        super().__init__(
            f"Missing required environment variable(s): {joined}. "
            f"Export them in your shell (do NOT hardcode secrets)."
        )


class MissingDependencyError(AuthError):
    """A required cryptographic library is not installed."""


class SigningError(AuthError):
    """A signature could not be produced (bad key, bad inputs, etc.)."""


# ---------------------------------------------------------------------------
# Credential loading
# ---------------------------------------------------------------------------

# Canonical env var names, grouped by venue.
ENV_KALSHI_KEY_ID = "KALSHI_API_KEY_ID"
ENV_KALSHI_KEY_PATH = "KALSHI_PRIVATE_KEY_PATH"
ENV_POLY_PRIVATE_KEY = "POLY_PRIVATE_KEY"
ENV_POLY_CLOB_API_KEY = "POLY_CLOB_API_KEY"

KALSHI_ENV_VARS = (ENV_KALSHI_KEY_ID, ENV_KALSHI_KEY_PATH)
POLY_ENV_VARS = (ENV_POLY_PRIVATE_KEY, ENV_POLY_CLOB_API_KEY)
ALL_ENV_VARS = KALSHI_ENV_VARS + POLY_ENV_VARS


@dataclass
class Credentials:
    """Raw credential values pulled from the environment.

    Private values are stored here but never printed by ``__repr__`` /
    ``summary()``; only presence and lengths are exposed.
    """
    kalshi_api_key_id: Optional[str] = None
    kalshi_private_key_path: Optional[str] = None
    poly_private_key: Optional[str] = None
    poly_clob_api_key: Optional[str] = None

    def __repr__(self) -> str:  # never leak secrets via repr
        return (
            "Credentials("
            f"kalshi_api_key_id={'set' if self.kalshi_api_key_id else 'unset'}, "
            f"kalshi_private_key_path={'set' if self.kalshi_private_key_path else 'unset'}, "
            f"poly_private_key={'set' if self.poly_private_key else 'unset'}, "
            f"poly_clob_api_key={'set' if self.poly_clob_api_key else 'unset'})"
        )

    def missing(self, required: Optional[List[str]] = None) -> List[str]:
        """Return the names of required env vars that are absent/empty."""
        wanted = required if required is not None else list(ALL_ENV_VARS)
        present = {
            ENV_KALSHI_KEY_ID: self.kalshi_api_key_id,
            ENV_KALSHI_KEY_PATH: self.kalshi_private_key_path,
            ENV_POLY_PRIVATE_KEY: self.poly_private_key,
            ENV_POLY_CLOB_API_KEY: self.poly_clob_api_key,
        }
        return [name for name in wanted if not present.get(name)]


def load_credentials(
    required: Optional[List[str]] = None, *, strict: bool = True
) -> Credentials:
    """Read credentials from the environment.

    Parameters
    ----------
    required:
        Env var names that must be present. Defaults to all four. Pass a
        subset (e.g. ``KALSHI_ENV_VARS``) to validate only one venue.
    strict:
        If True (default) raise ``MissingCredentialsError`` when any required
        var is missing. If False, return whatever was found and let callers
        decide.
    """
    creds = Credentials(
        kalshi_api_key_id=os.environ.get(ENV_KALSHI_KEY_ID) or None,
        kalshi_private_key_path=os.environ.get(ENV_KALSHI_KEY_PATH) or None,
        poly_private_key=os.environ.get(ENV_POLY_PRIVATE_KEY) or None,
        poly_clob_api_key=os.environ.get(ENV_POLY_CLOB_API_KEY) or None,
    )
    if strict:
        missing = creds.missing(required)
        if missing:
            raise MissingCredentialsError(missing)
    return creds


# ---------------------------------------------------------------------------
# Kalshi v2 — RSA-PSS request signing
# ---------------------------------------------------------------------------

class KalshiSigner:
    """Signs Kalshi v2 REST requests with RSA-PSS.

    Kalshi expects three headers per authenticated request:
        KALSHI-ACCESS-KEY        -> the API key id
        KALSHI-ACCESS-TIMESTAMP  -> current time in MILLISECONDS (string)
        KALSHI-ACCESS-SIGNATURE  -> base64( RSA-PSS-SHA256( msg ) )

    where ``msg = timestamp_ms + METHOD + path``. The path is the request path
    WITHOUT the query string (e.g. "/trade-api/v2/portfolio/balance") and must
    match the request's path byte-for-byte.
    """

    def __init__(self, api_key_id: str, private_key_path: str) -> None:
        if not _HAS_CRYPTOGRAPHY:
            raise MissingDependencyError(
                "The 'cryptography' package is required for Kalshi signing. "
                f"Install it with: pip install cryptography  ({_CRYPTOGRAPHY_IMPORT_ERR})"
            )
        if not api_key_id:
            raise MissingCredentialsError([ENV_KALSHI_KEY_ID])
        if not private_key_path:
            raise MissingCredentialsError([ENV_KALSHI_KEY_PATH])

        self.api_key_id = api_key_id
        self.private_key_path = private_key_path
        self._private_key = self._load_private_key(private_key_path)

    @staticmethod
    def _load_private_key(path: str) -> "RSAPrivateKey":
        """Load and validate the RSA private key from a PEM file."""
        try:
            with open(path, "rb") as fh:
                key_bytes = fh.read()
        except FileNotFoundError as exc:
            raise MissingCredentialsError([ENV_KALSHI_KEY_PATH]) from exc
        except OSError as exc:
            raise SigningError(
                f"Could not read Kalshi private key at {path!r}: {exc}"
            ) from exc

        try:
            # password=None: the key file is expected to be unencrypted. If you
            # use an encrypted PEM, load the passphrase from its own env var and
            # pass it here — never hardcode it.
            key = serialization.load_pem_private_key(key_bytes, password=None)
        except (ValueError, TypeError) as exc:
            raise SigningError(
                "Failed to parse Kalshi private key (is it an unencrypted "
                f"PEM RSA key?): {exc}"
            ) from exc

        if not isinstance(key, RSAPrivateKey):
            raise SigningError(
                "Kalshi requires an RSA private key for RSA-PSS signing; "
                f"got {type(key).__name__}."
            )
        return key

    @staticmethod
    def _now_ms() -> int:
        """Current time in integer milliseconds (Kalshi's expected unit)."""
        return int(time.time() * 1000)

    @staticmethod
    def _normalize_path(path: str) -> str:
        """Strip any query string; Kalshi signs the path only."""
        if not path.startswith("/"):
            path = "/" + path
        return path.split("?", 1)[0]

    def sign(
        self,
        method: str,
        path: str,
        timestamp_ms: Optional[int] = None,
    ) -> Dict[str, str]:
        """Return the three Kalshi auth headers for a request.

        Parameters
        ----------
        method:       HTTP verb, e.g. "GET" or "POST" (case-insensitive).
        path:         Request path, query string optional (it is stripped).
        timestamp_ms: Override the timestamp (mainly for testing). Defaults to
                      the current time in milliseconds.
        """
        ts = int(timestamp_ms) if timestamp_ms is not None else self._now_ms()
        norm_path = self._normalize_path(path)
        message = f"{ts}{method.upper()}{norm_path}".encode("utf-8")

        try:
            signature = self._private_key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    # Kalshi uses digest-length salt, NOT MAX_LENGTH.
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
        except Exception as exc:  # surface any backend signing failure cleanly
            raise SigningError(f"RSA-PSS signing failed: {exc}") from exc

        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": str(ts),
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("ascii"),
        }


# ---------------------------------------------------------------------------
# Polymarket — EIP-712 (L1) header boilerplate
# ---------------------------------------------------------------------------

# Polymarket CLOB L1 auth domain / typed-data schema. The signer proves control
# of the EOA by signing this ClobAuth struct; the CLOB then issues/uses L2 API
# credentials. chainId 137 = Polygon mainnet.
POLY_CLOB_DOMAIN = {
    "name": "ClobAuthDomain",
    "version": "1",
    "chainId": 137,
}
POLY_CLOB_AUTH_MESSAGE = (
    "This message attests that I control the given wallet"
)


class PolymarketAuth:
    """Prepares Polymarket CLOB L1 (EIP-712) authentication headers.

    This is intentionally boilerplate: it builds and signs the ClobAuth typed
    message and assembles the header dict the CLOB client expects. Plug the
    resulting headers into your request layer (or hand the signer to
    ``py-clob-client``) when you wire up live access.
    """

    def __init__(self, private_key: str, clob_api_key: Optional[str] = None) -> None:
        if not _HAS_ETH_ACCOUNT:
            raise MissingDependencyError(
                "The 'eth-account' package is required for Polymarket EIP-712 "
                f"signing. Install it with: pip install eth-account "
                f"({_ETH_ACCOUNT_IMPORT_ERR})"
            )
        if not private_key:
            raise MissingCredentialsError([ENV_POLY_PRIVATE_KEY])

        try:
            self._account = Account.from_key(private_key)
        except Exception as exc:
            raise SigningError(
                f"Invalid POLY_PRIVATE_KEY (could not derive account): {exc}"
            ) from exc

        self.address = self._account.address
        self.clob_api_key = clob_api_key

    @staticmethod
    def _now_s() -> int:
        """Polymarket L1 auth uses a SECONDS timestamp (string)."""
        return int(time.time())

    def _typed_data(self, timestamp: int, nonce: int) -> Dict:
        return {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                "ClobAuth": [
                    {"name": "address", "type": "address"},
                    {"name": "timestamp", "type": "string"},
                    {"name": "nonce", "type": "uint256"},
                    {"name": "message", "type": "string"},
                ],
            },
            "primaryType": "ClobAuth",
            "domain": POLY_CLOB_DOMAIN,
            "message": {
                "address": self.address,
                "timestamp": str(timestamp),
                "nonce": nonce,
                "message": POLY_CLOB_AUTH_MESSAGE,
            },
        }

    def build_l1_headers(
        self, timestamp: Optional[int] = None, nonce: int = 0
    ) -> Dict[str, str]:
        """Sign the ClobAuth struct and return L1 auth headers.

        Returns headers keyed as Polymarket's CLOB expects:
            POLY_ADDRESS, POLY_SIGNATURE, POLY_TIMESTAMP, POLY_NONCE
        plus POLY_API_KEY when a CLOB API key is configured.
        """
        ts = int(timestamp) if timestamp is not None else self._now_s()
        typed = self._typed_data(ts, nonce)

        try:
            signable = encode_typed_data(full_message=typed)
            signed = self._account.sign_message(signable)
            signature = signed.signature.hex()
            if not signature.startswith("0x"):
                signature = "0x" + signature
        except Exception as exc:
            raise SigningError(f"EIP-712 signing failed: {exc}") from exc

        headers = {
            "POLY_ADDRESS": self.address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": str(ts),
            "POLY_NONCE": str(nonce),
        }
        if self.clob_api_key:
            headers["POLY_API_KEY"] = self.clob_api_key
        return headers


# ---------------------------------------------------------------------------
# AuthManager — top-level orchestrator
# ---------------------------------------------------------------------------

class AuthManager:
    """Loads credentials and lazily builds per-venue signers.

    Construction validates the environment and reports clearly which variables
    (and which optional libraries) are missing, without ever printing secrets.
    """

    def __init__(
        self,
        *,
        require_kalshi: bool = True,
        require_polymarket: bool = True,
    ) -> None:
        self.errors: List[str] = []
        self._kalshi: Optional[KalshiSigner] = None
        self._poly: Optional[PolymarketAuth] = None

        required: List[str] = []
        if require_kalshi:
            required += list(KALSHI_ENV_VARS)
        if require_polymarket:
            required += list(POLY_ENV_VARS)

        # Load non-strict so we can collect ALL problems before raising.
        self.creds = load_credentials(strict=False)

        missing = self.creds.missing(required)
        if missing:
            self.errors.append(
                "Missing env var(s): " + ", ".join(missing)
            )

        # Build signers where possible; record (don't raise) per-venue issues.
        if require_kalshi and not self.creds.missing(list(KALSHI_ENV_VARS)):
            try:
                self._kalshi = KalshiSigner(
                    self.creds.kalshi_api_key_id,
                    self.creds.kalshi_private_key_path,
                )
            except AuthError as exc:
                self.errors.append(f"Kalshi init failed: {exc}")

        if require_polymarket and not self.creds.missing(list(POLY_ENV_VARS)):
            try:
                self._poly = PolymarketAuth(
                    self.creds.poly_private_key,
                    self.creds.poly_clob_api_key,
                )
            except AuthError as exc:
                self.errors.append(f"Polymarket init failed: {exc}")

    @property
    def kalshi(self) -> KalshiSigner:
        if self._kalshi is None:
            raise AuthError(
                "Kalshi signer unavailable. Reasons: "
                + ("; ".join(self.errors) or "not initialized")
            )
        return self._kalshi

    @property
    def polymarket(self) -> PolymarketAuth:
        if self._poly is None:
            raise AuthError(
                "Polymarket signer unavailable. Reasons: "
                + ("; ".join(self.errors) or "not initialized")
            )
        return self._poly

    def is_ready(self) -> bool:
        """True only if every requested signer initialized cleanly."""
        return not self.errors

    def summary(self) -> str:
        """Human-readable status with NO secret material."""
        lines = ["AuthManager status:"]
        lines.append(f"  cryptography installed : {_HAS_CRYPTOGRAPHY}")
        lines.append(f"  eth-account installed  : {_HAS_ETH_ACCOUNT}")
        lines.append(f"  Kalshi signer ready    : {self._kalshi is not None}")
        if self._poly is not None:
            lines.append(
                f"  Polymarket signer ready: True  (signer {self._poly.address})"
            )
        else:
            lines.append("  Polymarket signer ready: False")
        if self.errors:
            lines.append("  Issues:")
            lines.extend(f"    - {e}" for e in self.errors)
        else:
            lines.append("  Issues: none")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Self-check entrypoint
# ---------------------------------------------------------------------------

def _self_check() -> int:
    """Initialize the manager and report status; never prints secrets.

    Returns a process exit code: 0 if everything required is ready, 1 otherwise.
    """
    print("=" * 60)
    print(" auth_manager self-check (no secrets are printed)")
    print("=" * 60)
    try:
        mgr = AuthManager(require_kalshi=True, require_polymarket=True)
    except AuthError as exc:
        print(f"❌ Initialization error: {exc}")
        return 1

    print(mgr.summary())

    # If both signers are live, demonstrate header SHAPES on a dummy request,
    # printing only header names + value lengths (never the signatures' source).
    if mgr.is_ready():
        try:
            k_headers = mgr.kalshi.sign("GET", "/trade-api/v2/portfolio/balance")
            print("\n  Kalshi sample header keys:", list(k_headers))
            p_headers = mgr.polymarket.build_l1_headers()
            print("  Polymarket sample header keys:", list(p_headers))
            print("\n✅ All requested signers are ready.")
        except AuthError as exc:
            print(f"\n⚠️  Signer is initialized but signing failed: {exc}")
            return 1
        return 0

    print("\n⚠️  One or more signers are not ready (see Issues above).")
    return 1


if __name__ == "__main__":
    raise SystemExit(_self_check())
