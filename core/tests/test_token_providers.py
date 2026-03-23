"""Tests for framework.auth token providers.

Covers Claude Code and Codex OAuth token retrieval, expiry detection,
refresh flows, and credential persistence — all in isolation via mocks.
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Claude Code provider tests
# ---------------------------------------------------------------------------


class TestGetClaudeCodeToken:
    """Tests for get_claude_code_token() from framework.auth.claude_provider."""

    def test_returns_valid_token(self, monkeypatch):
        """Valid, non-expired token is returned as-is."""
        from framework.auth.claude_provider import get_claude_code_token

        future_ms = int((time.time() + 3600) * 1000)
        creds = {
            "claudeAiOauth": {
                "accessToken": "valid-token-abc",
                "expiresAt": future_ms,
                "refreshToken": "rt-123",
            }
        }
        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_credentials",
            lambda: creds,
        )
        assert get_claude_code_token() == "valid-token-abc"

    def test_returns_none_when_no_credentials(self, monkeypatch):
        """None returned when no credential file/keychain exists."""
        from framework.auth.claude_provider import get_claude_code_token

        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_credentials",
            lambda: None,
        )
        assert get_claude_code_token() is None

    def test_returns_none_when_no_access_token(self, monkeypatch):
        """None returned when credentials exist but accessToken is missing."""
        from framework.auth.claude_provider import get_claude_code_token

        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_credentials",
            lambda: {"claudeAiOauth": {}},
        )
        assert get_claude_code_token() is None

    def test_refreshes_expired_token(self, monkeypatch):
        """Expired token triggers refresh; new access_token is returned."""
        from framework.auth.claude_provider import get_claude_code_token

        past_ms = int((time.time() - 600) * 1000)
        creds = {
            "claudeAiOauth": {
                "accessToken": "expired-token",
                "expiresAt": past_ms,
                "refreshToken": "rt-old",
            }
        }
        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_credentials",
            lambda: creds,
        )
        monkeypatch.setattr(
            "framework.auth.claude_provider._refresh_claude_code_token",
            lambda rt: {"access_token": "fresh-token", "refresh_token": "rt-new", "expires_in": 7200},
        )
        save_called = {}
        monkeypatch.setattr(
            "framework.auth.claude_provider._save_refreshed_credentials",
            lambda td: save_called.update(td),
        )

        result = get_claude_code_token()
        assert result == "fresh-token"
        assert "access_token" in save_called

    def test_returns_expired_token_when_refresh_fails(self, monkeypatch):
        """Expired token returned as fallback when refresh fails."""
        from framework.auth.claude_provider import get_claude_code_token

        past_ms = int((time.time() - 600) * 1000)
        creds = {
            "claudeAiOauth": {
                "accessToken": "expired-token",
                "expiresAt": past_ms,
                "refreshToken": "rt-dead",
            }
        }
        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_credentials",
            lambda: creds,
        )
        monkeypatch.setattr(
            "framework.auth.claude_provider._refresh_claude_code_token",
            lambda rt: None,
        )
        assert get_claude_code_token() == "expired-token"

    def test_returns_expired_token_when_no_refresh_token(self, monkeypatch):
        """Expired token returned when no refresh token is available."""
        from framework.auth.claude_provider import get_claude_code_token

        past_ms = int((time.time() - 600) * 1000)
        creds = {
            "claudeAiOauth": {
                "accessToken": "expired-no-rt",
                "expiresAt": past_ms,
            }
        }
        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_credentials",
            lambda: creds,
        )
        assert get_claude_code_token() == "expired-no-rt"

    def test_proactive_refresh_within_buffer(self, monkeypatch):
        """Token within 5-min buffer window triggers proactive refresh."""
        from framework.auth.claude_provider import get_claude_code_token

        # Token expires in 2 minutes — inside the 5-min buffer
        almost_expired_ms = int((time.time() + 120) * 1000)
        creds = {
            "claudeAiOauth": {
                "accessToken": "almost-expired",
                "expiresAt": almost_expired_ms,
                "refreshToken": "rt-123",
            }
        }
        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_credentials",
            lambda: creds,
        )
        monkeypatch.setattr(
            "framework.auth.claude_provider._refresh_claude_code_token",
            lambda rt: {"access_token": "refreshed-proactive", "expires_in": 7200},
        )
        monkeypatch.setattr(
            "framework.auth.claude_provider._save_refreshed_credentials",
            lambda td: None,
        )
        assert get_claude_code_token() == "refreshed-proactive"


class TestReadClaudeCredentials:
    """Tests for credential reading from Keychain vs file."""

    def test_prefers_keychain_over_file(self, monkeypatch):
        """Keychain result is preferred when available."""
        from framework.auth.claude_provider import _read_claude_credentials

        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_keychain",
            lambda: {"claudeAiOauth": {"accessToken": "from-keychain"}},
        )
        result = _read_claude_credentials()
        assert result["claudeAiOauth"]["accessToken"] == "from-keychain"

    def test_falls_back_to_file(self, monkeypatch, tmp_path):
        """Falls back to file when Keychain returns None."""
        from framework.auth.claude_provider import _read_claude_credentials

        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_keychain",
            lambda: None,
        )
        creds_file = tmp_path / ".credentials.json"
        creds_file.write_text(json.dumps({"claudeAiOauth": {"accessToken": "from-file"}}))
        monkeypatch.setattr(
            "framework.auth.claude_provider.CLAUDE_CREDENTIALS_FILE",
            creds_file,
        )
        result = _read_claude_credentials()
        assert result["claudeAiOauth"]["accessToken"] == "from-file"

    def test_returns_none_when_file_missing(self, monkeypatch, tmp_path):
        """None returned when both Keychain and file are unavailable."""
        from framework.auth.claude_provider import _read_claude_credentials

        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_keychain",
            lambda: None,
        )
        monkeypatch.setattr(
            "framework.auth.claude_provider.CLAUDE_CREDENTIALS_FILE",
            tmp_path / "nonexistent.json",
        )
        assert _read_claude_credentials() is None

    def test_returns_none_on_malformed_json(self, monkeypatch, tmp_path):
        """None returned when credentials file contains invalid JSON."""
        from framework.auth.claude_provider import _read_claude_credentials

        monkeypatch.setattr(
            "framework.auth.claude_provider._read_claude_keychain",
            lambda: None,
        )
        creds_file = tmp_path / ".credentials.json"
        creds_file.write_text("{broken json")
        monkeypatch.setattr(
            "framework.auth.claude_provider.CLAUDE_CREDENTIALS_FILE",
            creds_file,
        )
        assert _read_claude_credentials() is None


class TestReadClaudeKeychain:
    """Tests for macOS Keychain access (platform-dependent)."""

    def test_returns_none_on_non_darwin(self, monkeypatch):
        """None returned on non-macOS platforms."""
        from framework.auth.claude_provider import _read_claude_keychain

        monkeypatch.setattr("platform.system", lambda: "Windows")
        assert _read_claude_keychain() is None

    @patch("subprocess.run")
    def test_returns_parsed_json_on_darwin(self, mock_run, monkeypatch):
        """Parsed JSON returned when Keychain entry exists on macOS."""
        from framework.auth.claude_provider import _read_claude_keychain

        monkeypatch.setattr("platform.system", lambda: "Darwin")
        monkeypatch.setattr("getpass.getuser", lambda: "testuser")
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"claudeAiOauth": {"accessToken": "kc-tok"}}),
        )
        result = _read_claude_keychain()
        assert result is not None
        assert result["claudeAiOauth"]["accessToken"] == "kc-tok"

    @patch("subprocess.run")
    def test_returns_none_on_keychain_miss(self, mock_run, monkeypatch):
        """None returned when Keychain entry doesn't exist."""
        from framework.auth.claude_provider import _read_claude_keychain

        monkeypatch.setattr("platform.system", lambda: "Darwin")
        monkeypatch.setattr("getpass.getuser", lambda: "testuser")
        mock_run.return_value = MagicMock(returncode=44, stdout="")
        assert _read_claude_keychain() is None


class TestSaveRefreshedCredentials:
    """Tests for writing refreshed token data back to storage."""

    def test_saves_to_file_when_keychain_unavailable(self, monkeypatch, tmp_path):
        """Refreshed credentials written to file when Keychain write fails."""
        from framework.auth.claude_provider import _save_refreshed_credentials

        creds_file = tmp_path / ".credentials.json"
        original = {"claudeAiOauth": {"accessToken": "old", "refreshToken": "rt"}}
        creds_file.write_text(json.dumps(original))
        monkeypatch.setattr("framework.auth.claude_provider.CLAUDE_CREDENTIALS_FILE", creds_file)
        monkeypatch.setattr("framework.auth.claude_provider._read_claude_credentials", lambda: original.copy())
        monkeypatch.setattr("framework.auth.claude_provider._save_claude_keychain", lambda c: False)

        token_data = {"access_token": "new-tok", "refresh_token": "new-rt", "expires_in": 3600}
        _save_refreshed_credentials(token_data)

        saved = json.loads(creds_file.read_text())
        assert saved["claudeAiOauth"]["accessToken"] == "new-tok"
        assert saved["claudeAiOauth"]["refreshToken"] == "new-rt"
        assert "expiresAt" in saved["claudeAiOauth"]


# ---------------------------------------------------------------------------
# Codex provider tests
# ---------------------------------------------------------------------------


class TestGetCodexToken:
    """Tests for get_codex_token() from framework.auth.codex_provider."""

    def test_returns_valid_token(self, monkeypatch):
        """Valid, non-expired Codex token is returned as-is."""
        from framework.auth.codex_provider import get_codex_token

        auth_data = {"tokens": {"access_token": "codex-valid"}, "last_refresh": time.time()}
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: auth_data)
        monkeypatch.setattr("framework.auth.codex_provider._is_codex_token_expired", lambda d: False)

        assert get_codex_token() == "codex-valid"

    def test_returns_none_when_no_auth_data(self, monkeypatch):
        """None returned when neither Keychain nor auth.json exists."""
        from framework.auth.codex_provider import get_codex_token

        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: None)

        assert get_codex_token() is None

    def test_returns_none_when_no_access_token(self, monkeypatch):
        """None returned when auth data exists but access_token is missing."""
        from framework.auth.codex_provider import get_codex_token

        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: {"tokens": {}})
        assert get_codex_token() is None

    def test_refreshes_expired_token(self, monkeypatch):
        """Expired Codex token triggers refresh; new access_token returned."""
        from framework.auth.codex_provider import get_codex_token

        auth_data = {
            "tokens": {"access_token": "expired-codex", "refresh_token": "rt-codex"},
        }
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: auth_data)
        monkeypatch.setattr("framework.auth.codex_provider._is_codex_token_expired", lambda d: True)
        monkeypatch.setattr(
            "framework.auth.codex_provider._refresh_codex_token",
            lambda rt: {"access_token": "fresh-codex"},
        )
        save_calls = []
        monkeypatch.setattr(
            "framework.auth.codex_provider._save_refreshed_codex_credentials",
            lambda ad, td: save_calls.append((ad, td)),
        )

        result = get_codex_token()
        assert result == "fresh-codex"
        assert len(save_calls) == 1

    def test_returns_expired_token_when_refresh_fails(self, monkeypatch):
        """Expired token returned as fallback when Codex refresh fails."""
        from framework.auth.codex_provider import get_codex_token

        auth_data = {
            "tokens": {"access_token": "expired-codex", "refresh_token": "rt-dead"},
        }
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: auth_data)
        monkeypatch.setattr("framework.auth.codex_provider._is_codex_token_expired", lambda d: True)
        monkeypatch.setattr("framework.auth.codex_provider._refresh_codex_token", lambda rt: None)

        assert get_codex_token() == "expired-codex"

    def test_returns_expired_token_when_no_refresh_token(self, monkeypatch):
        """Expired token returned when no refresh token is available."""
        from framework.auth.codex_provider import get_codex_token

        auth_data = {"tokens": {"access_token": "expired-no-rt"}}
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: auth_data)
        monkeypatch.setattr("framework.auth.codex_provider._is_codex_token_expired", lambda d: True)

        assert get_codex_token() == "expired-no-rt"

    def test_prefers_keychain_over_file(self, monkeypatch):
        """Keychain auth data is preferred when available."""
        from framework.auth.codex_provider import get_codex_token

        kc_data = {"tokens": {"access_token": "from-keychain"}}
        file_data = {"tokens": {"access_token": "from-file"}}
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: kc_data)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: file_data)
        monkeypatch.setattr("framework.auth.codex_provider._is_codex_token_expired", lambda d: False)

        assert get_codex_token() == "from-keychain"


class TestIsCodexTokenExpired:
    """Tests for Codex token expiry detection."""

    def test_not_expired_with_recent_last_refresh(self, monkeypatch):
        """Token with recent last_refresh is not expired."""
        from framework.auth.codex_provider import _is_codex_token_expired

        auth_data = {"last_refresh": time.time() - 60}
        assert _is_codex_token_expired(auth_data) is False

    def test_expired_with_old_last_refresh(self, monkeypatch):
        """Token with old last_refresh is expired."""
        from framework.auth.codex_provider import _is_codex_token_expired

        auth_data = {"last_refresh": time.time() - 7200}
        assert _is_codex_token_expired(auth_data) is True

    def test_expired_with_iso_timestamp(self):
        """Token with ISO 8601 last_refresh string (old) is expired."""
        from framework.auth.codex_provider import _is_codex_token_expired

        from datetime import datetime, timezone

        old_time = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
        auth_data = {"last_refresh": old_time}
        assert _is_codex_token_expired(auth_data) is True

    def test_not_expired_with_recent_iso_timestamp(self):
        """Token with recent ISO 8601 last_refresh string is not expired."""
        from datetime import datetime, timezone

        from framework.auth.codex_provider import _is_codex_token_expired

        recent_time = datetime.now(timezone.utc).isoformat()
        auth_data = {"last_refresh": recent_time}
        assert _is_codex_token_expired(auth_data) is False

    def test_falls_back_to_file_mtime(self, monkeypatch, tmp_path):
        """Falls back to auth.json mtime when last_refresh is absent."""
        from framework.auth.codex_provider import _is_codex_token_expired

        auth_file = tmp_path / "auth.json"
        auth_file.write_text("{}")
        monkeypatch.setattr("framework.auth.codex_provider.CODEX_AUTH_FILE", auth_file)

        # File was just created → mtime is recent → should not be expired
        assert _is_codex_token_expired({}) is False

    def test_returns_true_when_no_mtime_available(self, monkeypatch, tmp_path):
        """Returns True when last_refresh is absent and file doesn't exist."""
        from framework.auth.codex_provider import _is_codex_token_expired

        monkeypatch.setattr(
            "framework.auth.codex_provider.CODEX_AUTH_FILE",
            tmp_path / "nonexistent.json",
        )
        assert _is_codex_token_expired({}) is True

    def test_returns_true_on_invalid_iso_string(self):
        """Returns True when last_refresh is an unparseable string."""
        from framework.auth.codex_provider import _is_codex_token_expired

        assert _is_codex_token_expired({"last_refresh": "not-a-date"}) is True


class TestGetCodexAccountId:
    """Tests for get_codex_account_id()."""

    def test_returns_explicit_account_id(self, monkeypatch):
        """Returns tokens.account_id when present."""
        from framework.auth.codex_provider import get_codex_account_id

        auth_data = {"tokens": {"account_id": "acct-123", "access_token": "tok"}}
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: auth_data)

        assert get_codex_account_id() == "acct-123"

    def test_extracts_account_id_from_jwt(self, monkeypatch):
        """Falls back to JWT extraction when tokens.account_id is missing."""
        from framework.auth.codex_provider import get_codex_account_id

        payload = {
            "https://api.openai.com/auth": {"chatgpt_account_id": "jwt-acct-456"}
        }
        payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        fake_jwt = f"header.{payload_b64}.signature"

        auth_data = {"tokens": {"access_token": fake_jwt}}
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: auth_data)

        assert get_codex_account_id() == "jwt-acct-456"

    def test_returns_none_when_no_auth_data(self, monkeypatch):
        """None returned when no Codex credentials are available."""
        from framework.auth.codex_provider import get_codex_account_id

        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: None)

        assert get_codex_account_id() is None

    def test_returns_none_on_malformed_jwt(self, monkeypatch):
        """None returned when access_token is not a valid JWT."""
        from framework.auth.codex_provider import get_codex_account_id

        auth_data = {"tokens": {"access_token": "not.a.valid-jwt"}}
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_keychain", lambda: None)
        monkeypatch.setattr("framework.auth.codex_provider._read_codex_auth_file", lambda: auth_data)

        assert get_codex_account_id() is None


class TestGetAccountIdFromJwt:
    """Tests for JWT account_id extraction helper."""

    def test_extracts_account_id(self):
        """Successfully extracts chatgpt_account_id from valid JWT."""
        from framework.auth.codex_provider import _get_account_id_from_jwt

        payload = {
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct-789"}
        }
        payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        jwt = f"eyJhbGciOiJSUzI1NiJ9.{payload_b64}.fakesig"

        assert _get_account_id_from_jwt(jwt) == "acct-789"

    def test_returns_none_for_missing_claim(self):
        """None returned when auth claim is missing from JWT payload."""
        from framework.auth.codex_provider import _get_account_id_from_jwt

        payload = {"sub": "user123"}
        payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        jwt = f"header.{payload_b64}.sig"

        assert _get_account_id_from_jwt(jwt) is None

    def test_returns_none_for_non_jwt(self):
        """None returned for strings that aren't JWTs."""
        from framework.auth.codex_provider import _get_account_id_from_jwt

        assert _get_account_id_from_jwt("plain-api-key") is None
        assert _get_account_id_from_jwt("") is None

    def test_returns_none_for_empty_account_id(self):
        """None returned when chatgpt_account_id is empty string."""
        from framework.auth.codex_provider import _get_account_id_from_jwt

        payload = {
            "https://api.openai.com/auth": {"chatgpt_account_id": ""}
        }
        payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        jwt = f"h.{payload_b64}.s"

        assert _get_account_id_from_jwt(jwt) is None


class TestReadCodexKeychain:
    """Tests for macOS Keychain access for Codex."""

    def test_returns_none_on_non_darwin(self, monkeypatch):
        """None returned on non-macOS platforms."""
        from framework.auth.codex_provider import _read_codex_keychain

        monkeypatch.setattr("platform.system", lambda: "Linux")
        assert _read_codex_keychain() is None

    @patch("subprocess.run")
    def test_returns_parsed_json_on_darwin(self, mock_run, monkeypatch):
        """Parsed JSON returned when Keychain entry exists on macOS."""
        from framework.auth.codex_provider import _read_codex_keychain

        monkeypatch.setattr("platform.system", lambda: "Darwin")
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"tokens": {"access_token": "kc-codex"}}),
        )
        result = _read_codex_keychain()
        assert result is not None
        assert result["tokens"]["access_token"] == "kc-codex"


class TestReadCodexAuthFile:
    """Tests for file-based Codex auth reading."""

    def test_reads_valid_auth_file(self, monkeypatch, tmp_path):
        """Successfully reads a valid auth.json."""
        from framework.auth.codex_provider import _read_codex_auth_file

        auth_file = tmp_path / "auth.json"
        auth_file.write_text(json.dumps({"tokens": {"access_token": "file-tok"}}))
        monkeypatch.setattr("framework.auth.codex_provider.CODEX_AUTH_FILE", auth_file)

        result = _read_codex_auth_file()
        assert result["tokens"]["access_token"] == "file-tok"

    def test_returns_none_when_file_missing(self, monkeypatch, tmp_path):
        """None returned when auth.json doesn't exist."""
        from framework.auth.codex_provider import _read_codex_auth_file

        monkeypatch.setattr(
            "framework.auth.codex_provider.CODEX_AUTH_FILE",
            tmp_path / "missing.json",
        )
        assert _read_codex_auth_file() is None

    def test_returns_none_on_malformed_json(self, monkeypatch, tmp_path):
        """None returned when auth.json contains invalid JSON."""
        from framework.auth.codex_provider import _read_codex_auth_file

        auth_file = tmp_path / "auth.json"
        auth_file.write_text("not-json{")
        monkeypatch.setattr("framework.auth.codex_provider.CODEX_AUTH_FILE", auth_file)

        assert _read_codex_auth_file() is None


class TestSaveRefreshedCodexCredentials:
    """Tests for writing refreshed Codex token data."""

    def test_writes_refreshed_tokens_to_file(self, monkeypatch, tmp_path):
        """Refreshed token data written to auth.json."""
        from framework.auth.codex_provider import _save_refreshed_codex_credentials

        auth_file = tmp_path / "auth.json"
        monkeypatch.setattr("framework.auth.codex_provider.CODEX_AUTH_FILE", auth_file)

        auth_data = {"tokens": {"access_token": "old", "refresh_token": "old-rt"}}
        token_data = {
            "access_token": "new-tok",
            "refresh_token": "new-rt",
            "id_token": "new-id",
        }
        _save_refreshed_codex_credentials(auth_data, token_data)

        saved = json.loads(auth_file.read_text())
        assert saved["tokens"]["access_token"] == "new-tok"
        assert saved["tokens"]["refresh_token"] == "new-rt"
        assert saved["tokens"]["id_token"] == "new-id"
        assert "last_refresh" in saved


class TestGetCodexKeychainAccount:
    """Tests for Codex Keychain account name computation."""

    def test_returns_deterministic_account_name(self):
        """Account name is deterministic for the same home directory."""
        from framework.auth.codex_provider import _get_codex_keychain_account

        result = _get_codex_keychain_account()
        assert result.startswith("cli|")
        assert len(result) == 4 + 16  # "cli|" + 16-char hex digest

        # Deterministic: calling again gives the same result
        assert _get_codex_keychain_account() == result


# ---------------------------------------------------------------------------
# Auth module public API tests
# ---------------------------------------------------------------------------


class TestAuthModulePublicAPI:
    """Tests that framework.auth exposes the expected public API."""

    def test_exports_claude_code_token(self):
        from framework.auth import get_claude_code_token

        assert callable(get_claude_code_token)

    def test_exports_codex_token(self):
        from framework.auth import get_codex_token

        assert callable(get_codex_token)

    def test_exports_codex_account_id(self):
        from framework.auth import get_codex_account_id

        assert callable(get_codex_account_id)

    def test_all_exports_match(self):
        import framework.auth

        assert set(framework.auth.__all__) == {
            "get_claude_code_token",
            "get_codex_token",
            "get_codex_account_id",
        }
