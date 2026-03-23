"""Claude Code subscription token provider.

Reads and refreshes OAuth tokens from the Claude Code CLI credential
store (macOS Keychain or ``~/.claude/.credentials.json``).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

CLAUDE_CREDENTIALS_FILE = Path.home() / ".claude" / ".credentials.json"
CLAUDE_OAUTH_TOKEN_URL = "https://console.anthropic.com/v1/oauth/token"
CLAUDE_OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
CLAUDE_KEYCHAIN_SERVICE = "Claude Code-credentials"

# Buffer in seconds before token expiry to trigger a proactive refresh
_TOKEN_REFRESH_BUFFER_SECS = 300  # 5 minutes


# ---------------------------------------------------------------------------
# Keychain helpers (macOS only)
# ---------------------------------------------------------------------------


def _read_claude_keychain() -> dict | None:
    """Read Claude Code credentials from macOS Keychain.

    Returns the parsed JSON dict, or None if not on macOS or entry missing.
    """
    import getpass
    import platform
    import subprocess

    if platform.system() != "Darwin":
        return None

    try:
        account = getpass.getuser()
        result = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-s",
                CLAUDE_KEYCHAIN_SERVICE,
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
        logger.debug("Claude keychain read failed: %s", exc)
        return None


def _save_claude_keychain(creds: dict) -> bool:
    """Write Claude Code credentials to macOS Keychain. Returns True on success."""
    import getpass
    import platform
    import subprocess

    if platform.system() != "Darwin":
        return False

    try:
        account = getpass.getuser()
        data = json.dumps(creds)
        result = subprocess.run(
            [
                "security",
                "add-generic-password",
                "-U",
                "-s",
                CLAUDE_KEYCHAIN_SERVICE,
                "-a",
                account,
                "-w",
                data,
            ],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError) as exc:
        logger.debug("Claude keychain write failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Credential reading / writing
# ---------------------------------------------------------------------------


def _read_claude_credentials() -> dict | None:
    """Read Claude Code credentials from Keychain (macOS) or file (Linux/Windows)."""
    creds = _read_claude_keychain()
    if creds:
        return creds

    if not CLAUDE_CREDENTIALS_FILE.exists():
        return None

    try:
        with open(CLAUDE_CREDENTIALS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


# ---------------------------------------------------------------------------
# Token refresh
# ---------------------------------------------------------------------------


def _refresh_claude_code_token(refresh_token: str) -> dict | None:
    """Refresh the Claude Code OAuth token using the refresh token.

    POSTs to the Anthropic OAuth token endpoint with form-urlencoded data
    (per OAuth 2.0 RFC 6749 Section 4.1.3).

    Returns:
        Dict with new token data (access_token, refresh_token, expires_in)
        on success, None on failure.
    """
    import urllib.error
    import urllib.parse
    import urllib.request

    data = urllib.parse.urlencode(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CLAUDE_OAUTH_CLIENT_ID,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        CLAUDE_OAUTH_TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError, OSError) as exc:
        logger.debug("Claude Code token refresh failed: %s", exc)
        return None


def _save_refreshed_credentials(token_data: dict) -> None:
    """Write refreshed token data back to Keychain (macOS) or credentials file."""
    import time

    creds = _read_claude_credentials()
    if not creds:
        return

    try:
        oauth = creds.get("claudeAiOauth", {})
        oauth["accessToken"] = token_data["access_token"]
        if "refresh_token" in token_data:
            oauth["refreshToken"] = token_data["refresh_token"]
        if "expires_in" in token_data:
            oauth["expiresAt"] = int((time.time() + token_data["expires_in"]) * 1000)
        creds["claudeAiOauth"] = oauth

        if _save_claude_keychain(creds):
            logger.debug("Claude Code credentials refreshed in Keychain")
            return

        if CLAUDE_CREDENTIALS_FILE.exists():
            with open(CLAUDE_CREDENTIALS_FILE, "w", encoding="utf-8") as f:
                json.dump(creds, f, indent=2)
            logger.debug("Claude Code credentials refreshed in file")
    except (json.JSONDecodeError, OSError, KeyError) as exc:
        logger.debug("Failed to save refreshed credentials: %s", exc)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_claude_code_token() -> str | None:
    """Get the OAuth token from Claude Code subscription with auto-refresh.

    Reads from macOS Keychain (on Darwin) or ~/.claude/.credentials.json
    (on Linux/Windows), as created by the Claude Code CLI.

    If the token is expired or close to expiry, attempts an automatic
    refresh using the stored refresh token.

    Returns:
        The access token if available, None otherwise.
    """
    import time

    creds = _read_claude_credentials()
    if not creds:
        return None

    oauth = creds.get("claudeAiOauth", {})
    access_token = oauth.get("accessToken")
    if not access_token:
        return None

    expires_at_ms = oauth.get("expiresAt", 0)
    now_ms = int(time.time() * 1000)
    buffer_ms = _TOKEN_REFRESH_BUFFER_SECS * 1000

    if expires_at_ms > now_ms + buffer_ms:
        return access_token

    # Token is expired or near expiry — attempt refresh
    refresh_token = oauth.get("refreshToken")
    if not refresh_token:
        logger.warning("Claude Code token expired and no refresh token available")
        return access_token  # Return expired token; it may still work briefly

    logger.info("Claude Code token expired or near expiry, refreshing...")
    token_data = _refresh_claude_code_token(refresh_token)

    if token_data and "access_token" in token_data:
        _save_refreshed_credentials(token_data)
        return token_data["access_token"]

    logger.warning("Claude Code token refresh failed. Run 'claude' to re-authenticate.")
    return access_token
