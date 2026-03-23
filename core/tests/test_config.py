"""Tests for framework/config.py - Hive configuration loading."""

import json
import logging

from framework.config import get_api_key, get_api_base, get_hive_config, get_llm_extra_kwargs


class TestGetHiveConfig:
    """Test get_hive_config() logs warnings on parse errors."""

    def test_logs_warning_on_malformed_json(self, tmp_path, monkeypatch, caplog):
        """Test that malformed JSON logs warning and returns empty dict."""
        config_file = tmp_path / "configuration.json"
        config_file.write_text('{"broken": }')

        monkeypatch.setattr("framework.config.HIVE_CONFIG_FILE", config_file)

        with caplog.at_level(logging.WARNING):
            result = get_hive_config()

        assert result == {}
        assert "Failed to load Hive config" in caplog.text
        assert str(config_file) in caplog.text


# ---------------------------------------------------------------------------
# get_api_key() tests — verify token resolution priority
# ---------------------------------------------------------------------------


def _write_config(tmp_path, monkeypatch, config: dict) -> None:
    """Helper: write a config dict and point HIVE_CONFIG_FILE at it."""
    config_file = tmp_path / "configuration.json"
    config_file.write_text(json.dumps(config))
    monkeypatch.setattr("framework.config.HIVE_CONFIG_FILE", config_file)


class TestGetApiKey:
    """Test get_api_key() token resolution priority."""

    def test_claude_subscription_returns_token(self, tmp_path, monkeypatch):
        """Claude Code subscription token is returned when configured."""
        _write_config(tmp_path, monkeypatch, {"llm": {"use_claude_code_subscription": True}})
        monkeypatch.setattr(
            "framework.auth.get_claude_code_token",
            lambda: "claude-tok-123",
        )
        assert get_api_key() == "claude-tok-123"

    def test_codex_subscription_returns_token(self, tmp_path, monkeypatch):
        """Codex subscription token is returned when configured."""
        _write_config(tmp_path, monkeypatch, {"llm": {"use_codex_subscription": True}})
        monkeypatch.setattr(
            "framework.auth.get_codex_token",
            lambda: "codex-tok-456",
        )
        assert get_api_key() == "codex-tok-456"

    def test_env_var_fallback(self, tmp_path, monkeypatch):
        """Falls back to env var when no subscription is configured."""
        _write_config(tmp_path, monkeypatch, {"llm": {"api_key_env_var": "MY_API_KEY"}})
        monkeypatch.setenv("MY_API_KEY", "env-key-789")
        assert get_api_key() == "env-key-789"

    def test_returns_none_when_no_config(self, tmp_path, monkeypatch):
        """None returned when no subscription and no env var is set."""
        _write_config(tmp_path, monkeypatch, {})
        assert get_api_key() is None

    def test_claude_priority_over_codex(self, tmp_path, monkeypatch):
        """Claude subscription is checked before Codex when both are configured."""
        _write_config(tmp_path, monkeypatch, {
            "llm": {
                "use_claude_code_subscription": True,
                "use_codex_subscription": True,
            },
        })
        monkeypatch.setattr(
            "framework.auth.get_claude_code_token",
            lambda: "claude-wins",
        )
        monkeypatch.setattr(
            "framework.auth.get_codex_token",
            lambda: "codex-loses",
        )
        assert get_api_key() == "claude-wins"

    def test_falls_through_when_claude_returns_none(self, tmp_path, monkeypatch):
        """Falls through to Codex when Claude token returns None."""
        _write_config(tmp_path, monkeypatch, {
            "llm": {
                "use_claude_code_subscription": True,
                "use_codex_subscription": True,
            },
        })
        monkeypatch.setattr(
            "framework.auth.get_claude_code_token",
            lambda: None,
        )
        monkeypatch.setattr(
            "framework.auth.get_codex_token",
            lambda: "codex-fallback",
        )
        assert get_api_key() == "codex-fallback"

    def test_env_var_not_set_returns_none(self, tmp_path, monkeypatch):
        """None returned when api_key_env_var is configured but not in environment."""
        _write_config(tmp_path, monkeypatch, {"llm": {"api_key_env_var": "MISSING_KEY"}})
        monkeypatch.delenv("MISSING_KEY", raising=False)
        assert get_api_key() is None


# ---------------------------------------------------------------------------
# get_api_base() tests
# ---------------------------------------------------------------------------


class TestGetApiBase:
    """Test get_api_base() returns correct endpoints."""

    def test_codex_subscription_returns_chatgpt_backend(self, tmp_path, monkeypatch):
        """Codex subscription routes through ChatGPT backend."""
        _write_config(tmp_path, monkeypatch, {"llm": {"use_codex_subscription": True}})
        assert get_api_base() == "https://chatgpt.com/backend-api/codex"

    def test_custom_api_base(self, tmp_path, monkeypatch):
        """Custom api_base from config is returned."""
        _write_config(tmp_path, monkeypatch, {"llm": {"api_base": "http://localhost:11434"}})
        assert get_api_base() == "http://localhost:11434"

    def test_returns_none_by_default(self, tmp_path, monkeypatch):
        """None returned when no api_base is configured."""
        _write_config(tmp_path, monkeypatch, {})
        assert get_api_base() is None


# ---------------------------------------------------------------------------
# get_llm_extra_kwargs() tests
# ---------------------------------------------------------------------------


class TestGetLlmExtraKwargs:
    """Test get_llm_extra_kwargs() returns correct headers."""

    def test_claude_subscription_adds_bearer_header(self, tmp_path, monkeypatch):
        """Claude subscription adds lowercase authorization Bearer header."""
        _write_config(tmp_path, monkeypatch, {"llm": {"use_claude_code_subscription": True}})
        monkeypatch.setattr(
            "framework.auth.get_claude_code_token",
            lambda: "claude-tok",
        )
        result = get_llm_extra_kwargs()
        assert result["extra_headers"]["authorization"] == "Bearer claude-tok"

    def test_codex_subscription_adds_headers_and_store(self, tmp_path, monkeypatch):
        """Codex subscription adds Authorization, User-Agent, and store=False."""
        _write_config(tmp_path, monkeypatch, {"llm": {"use_codex_subscription": True}})
        monkeypatch.setattr(
            "framework.auth.get_codex_token",
            lambda: "codex-tok",
        )
        monkeypatch.setattr(
            "framework.auth.get_codex_account_id",
            lambda: "acct-123",
        )
        result = get_llm_extra_kwargs()
        assert result["extra_headers"]["Authorization"] == "Bearer codex-tok"
        assert result["extra_headers"]["User-Agent"] == "CodexBar"
        assert result["extra_headers"]["ChatGPT-Account-Id"] == "acct-123"
        assert result["store"] is False

    def test_codex_subscription_without_account_id(self, tmp_path, monkeypatch):
        """Codex subscription works without account_id (header omitted)."""
        _write_config(tmp_path, monkeypatch, {"llm": {"use_codex_subscription": True}})
        monkeypatch.setattr(
            "framework.auth.get_codex_token",
            lambda: "codex-tok",
        )
        monkeypatch.setattr(
            "framework.auth.get_codex_account_id",
            lambda: None,
        )
        result = get_llm_extra_kwargs()
        assert "ChatGPT-Account-Id" not in result["extra_headers"]

    def test_returns_empty_dict_by_default(self, tmp_path, monkeypatch):
        """Empty dict returned when no subscription is configured."""
        _write_config(tmp_path, monkeypatch, {})
        assert get_llm_extra_kwargs() == {}
