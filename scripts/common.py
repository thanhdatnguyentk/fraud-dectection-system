"""Common utilities shared across ingestion / synthesis / feature jobs.

Exports
--------
* :func:`hmac_hash`     — deterministic PII hashing (HMAC-SHA256, hex).
* :func:`new_ulid`      — monotonically-sortable 26-char ID.
* :func:`load_settings` — read .env + sensible defaults for local dev.

These are intentionally dependency-light (stdlib + pydantic) so they're
importable in every phase of the project, including the data generator
which may run before any heavyweight dep is installed.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field


# =============================================================================
# Settings
# =============================================================================

class Settings(BaseModel):
    pii_hmac_key: str = Field(
        default="0" * 64,
        description="Hex-encoded 32-byte secret for HMAC-SHA256. Rotate quarterly.",
    )
    ieee_cis_epoch: datetime = Field(
        default=datetime(2017, 12, 1, tzinfo=timezone.utc),
        description="Reference timestamp for IEEE-CIS TransactionDT (seconds since this UTC instant).",
    )
    raw_dir: Path = Path("data/raw")
    canonical_dir: Path = Path("data/canonical")
    artifacts_dir: Path = Path("data/artifacts")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            pii_hmac_key=os.environ.get("PII_HMAC_KEY", "0" * 64),
            ieee_cis_epoch=datetime.fromisoformat(
                os.environ.get("IEEE_CIS_EPOCH", "2017-12-01T00:00:00+00:00")
            ),
            raw_dir=Path(os.environ.get("RAW_DIR", "data/raw")),
            canonical_dir=Path(os.environ.get("CANONICAL_DIR", "data/canonical")),
            artifacts_dir=Path(os.environ.get("ARTIFACTS_DIR", "data/artifacts")),
        )


def load_settings() -> Settings:
    """Load .env (best-effort) then return Settings.

    ``.env`` is loaded only if present.  Production environments usually inject
    variables via the orchestrator (k8s, airflow, …) so this is a developer
    convenience only.
    """
    env_path = Path(".env")
    if env_path.exists():
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv(env_path)
        except ImportError:  # dotenv is optional
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    return Settings.from_env()


# =============================================================================
# Hashing & ID generation
# =============================================================================

def _key_bytes(s: str) -> bytes:
    """Decode a 64-char hex string into 32 bytes; pad/zero-pad if malformed."""
    try:
        b = bytes.fromhex(s)
        if len(b) == 32:
            return b
    except ValueError:
        pass
    # Dev fallback: derive a stable 32-byte key from arbitrary string.
    return hashlib.sha256(s.encode("utf-8")).digest()


def hmac_hash(value: str | None, key: str) -> str:
    """Return a 64-char hex HMAC-SHA256 of ``value`` (or empty string if None).

    The output is the canonical representation of an opaque ID and **must not**
    be reversible to the original value.  Use this for any PII field that
    downstream joins require equality on.
    """
    if value is None:
        value = ""
    digest = hmac.new(_key_bytes(key), str(value).encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()


# Crockford base32 alphabet (no I, L, O, U) — same one the `ulid-py` package uses.
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def new_ulid() -> str:
    """Return a 26-char ULID.  Time-ordered (first 10 chars encode ms since epoch).

    We hand-roll a minimal version to avoid the extra dependency in this
    base module.  Good enough for offline / batch use.
    """
    ts_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_bytes = uuid.uuid4().bytes

    # Encode 48-bit timestamp to 10 base32 chars
    chars = []
    v = ts_ms
    for _ in range(10):
        chars.append(_CROCKFORD[v & 31])
        v >>= 5
    ts_str = "".join(reversed(chars))

    # Encode 80 random bits to 16 base32 chars
    rand_int = int.from_bytes(rand_bytes, "big")
    chars = []
    for _ in range(16):
        chars.append(_CROCKFORD[rand_int & 31])
        rand_int >>= 5
    rand_str = "".join(reversed(chars))

    return ts_str + rand_str


# =============================================================================
# Misc
# =============================================================================

def project_root() -> Path:
    """Return the absolute path of the project root (where this file's parent lives)."""
    return Path(__file__).resolve().parent.parent


__all__ = [
    "Settings",
    "load_settings",
    "hmac_hash",
    "new_ulid",
    "project_root",
]