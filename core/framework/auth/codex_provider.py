"""Codex (OpenAI) subscription token provider.

Reads and refreshes OAuth tokens from the Codex CLI credential store
(macOS Keychain or ``~/.codex/auth.json``).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC
from pathlib import Path

logger = logging.getLogger(__name__)

CODEX_AUTH_FILE = Path.home() / ".codex" / "auth.json"
CODEX_OAUTH_TOKEN_URL = "https://auth.openai.com/oauth/token"
CODEX_OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
CODEX_KEYCHAIN_SERVICE = "Codex Auth"
_CODEX_TOKEN_LIFETIME_SECS = 3600  # 1 hour (no explicit expiry field)

# Buffer in seconds before token expiry to trigger a proactive refresh
_TOKEN_REFRESH_BUFFER_SECS = 300  # 5 minutes


# ---------------------------------------------------------------------------
# Keychain helpers (macOS only)
# ---------------------------------------------------------------------------


def _get_codex_keychain_account() -> str:
    """Compute the macOS Keychain account name used by the Codex CLI.

    The Codex CLI stores credentials under the account
    ``cli|<sha256(~/.codex)[:16]>`` in the ``Codex Auth`` service.
    """
    import hashlib

    codex_dir = str(Path.home() / ".codex")
    digest = hashlib.sha256(codex_dir.encode()).hexdigest()[:16]
    return f"cli|{digest}"


def _read_codex_keychain() -> dict | None:
    """Read Codex auth data from macOS Keychain (macOS only).

    Returns the parsed JSON from the Keychain entry, or None if not
    available (wrong platform, entry missing, etc.).
    """
    import platform
    import subprocess

    if platform.system() != "Darwin":
        return None

    try:
        account = _get_codex_keychain_account()
        result = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-s",
                CODEX_KEYCHAIN_SERVICE,
                "-a",
                account,
                "-w",
            ],
            capture_output=True,
            encoding="utf-8",
            timeout=5,
        )
        if result.returncode != 0:
            return None
        raw = result.stdout.strip()
        if not raw:
            return None
        return json.loads(raw)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError) as exc:
        logger.debug("Codex keychain read failed: %s", exc)
        return None


def _read_codex_auth_file() -> dict | None:
    """Read Codex auth data from ~/.codex/auth.json (fallback)."""
    if not CODEX_AUTH_FILE.exists():
        return None
    try:
        with open(CODEX_AUTH_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


# ---------------------------------------------------------------------------
# Token expiry check
# ---------------------------------------------------------------------------


def _is_codex_token_expired(auth_data: dict) -> bool:
    """Check whether the Codex token is expired or close to expiry.

    The Codex auth.json has no explicit ``expiresAt`` field, so we infer
    expiry as ``last_refresh + _CODEX_TOKEN_LIFETIME_SECS``.  Falls back
    to the file mtime when ``last_refresh`` is absent.
    """
    import time
    from datetime import datetime

    now = time.time()
    last_refresh = auth_data.get("last_refresh")

    if last_refresh is None:
        try:
            last_refresh = CODEX_AUTH_FILE.stat().st_mtime
        except OSError:
            return True
    elif isinstance(last_refresh, str):
        # Codex stores last_refresh as an ISO 8601 timestamp string —
        # convert to Unix epoch float for arithmetic.
        try:
            last_refresh = datetime.fromisoformat(last_refresh.replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError):
            return True

    expires_at = last_refresh + _CODEX_TOKEN_LIFETIME_SECS
    return now >= (expires_at - _TOKEN_REFRESH_BUFFER_SECS)


# ---------------------------------------------------------------------------
# Token refresh
# ---------------------------------------------------------------------------


def _refresh_codex_token(refresh_token: str) -> dict | None:
    """Refresh the Codex OAuth token using the refresh token.

    POSTs to the OpenAI auth endpoint with form-urlencoded data.

    Returns:
        Dict with new token data on success, None on failure.
    """
    import urllib.error
    import urllib.parse
    import urllib.request

    data = urllib.parse.urlencode(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CODEX_OAUTH_CLIENT_ID,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        CODEX_OAUTH_TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError, OSError) as exc:
        logger.debug("Codex token refresh failed: %s", exc)
        return None


def _save_refreshed_codex_credentials(auth_data: dict, token_data: dict) -> None:
    """Write refreshed tokens back to ~/.codex/auth.json only (not Keychain).

    The Codex CLI manages its own Keychain entries, so we only update the
    file-based credentials.
    """
    from datetime import datetime

    try:
        tokens = auth_data.get("tokens", {})
        tokens["access_token"] = token_data["access_token"]
        if "refresh_token" in token_data:
            tokens["refresh_token"] = token_data["refresh_token"]
        if "id_token" in token_data:
            tokens["id_token"] = token_data["id_token"]
        auth_data["tokens"] = tokens
        auth_data["last_refresh"] = datetime.now(UTC).isoformat()

        CODEX_AUTH_FILE.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(CODEX_AUTH_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(auth_data, f, indent=2)
        logger.debug("Codex credentials refreshed successfully")
    except (OSError, KeyError) as exc:
        logger.debug("Failed to save refreshed Codex credentials: %s", exc)


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def _get_account_id_from_jwt(access_token: str) -> str | None:
    """Extract the ChatGPT account_id from the access token JWT.

    The OpenAI access token JWT contains a claim at
    ``https://api.openai.com/auth`` with a ``chatgpt_account_id`` field.
    This is used as a fallback when the auth.json doesn't store the
    account_id explicitly.
    """
    import base64

    try:
        parts = access_token.split(".")
        if len(parts) != 3:
            return None
        payload = parts[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        decoded = base64.urlsafe_b64decode(payload)
        claims = json.loads(decoded)
        auth = claims.get("https://api.openai.com/auth")
        if isinstance(auth, dict):
            account_id = auth.get("chatgpt_account_id")
            if isinstance(account_id, str) and account_id:
                return account_id
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_codex_token() -> str | None:
    """Get the OAuth token from Codex subscription with auto-refresh.

    Reads from macOS Keychain first, then falls back to
    ``~/.codex/auth.json``.  If the token is expired or close to
    expiry, attempts an automatic refresh.

    Returns:
        The access token if available, None otherwise.
    """
    auth_data = _read_codex_keychain() or _read_codex_auth_file()
    if not auth_data:
        return None

    tokens = auth_data.get("tokens", {})
    access_token = tokens.get("access_token")
    if not access_token:
        return None

    if not _is_codex_token_expired(auth_data):
        return access_token

    # Token is expired or near expiry — attempt refresh
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        logger.warning("Codex token expired and no refresh token available")
        return access_token  # Return expired token; it may still work briefly

    logger.info("Codex token expired or near expiry, refreshing...")
    token_data = _refresh_codex_token(refresh_token)

    if token_data and "access_token" in token_data:
        _save_refreshed_codex_credentials(auth_data, token_data)
        return token_data["access_token"]

    logger.warning("Codex token refresh failed. Run 'codex' to re-authenticate.")
    return access_token


def get_codex_account_id() -> str | None:
    """Extract the account ID from Codex auth data for the ChatGPT-Account-Id header.

    Checks the ``tokens.account_id`` field first, then falls back to
    decoding the account ID from the access token JWT.

    Returns:
        The account_id string if available, None otherwise.
    """
    auth_data = _read_codex_keychain() or _read_codex_auth_file()
    if not auth_data:
        return None
    tokens = auth_data.get("tokens", {})
    account_id = tokens.get("account_id")
    if account_id:
        return account_id
    access_token = tokens.get("access_token")
    if access_token:
        return _get_account_id_from_jwt(access_token)
    return None
