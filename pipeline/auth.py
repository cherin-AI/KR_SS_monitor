"""
pipeline/auth.py
Token issuance and caching for KIS Open API.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml


class KISAPIError(Exception):
    def __init__(self, msg_cd: str, msg1: str) -> None:
        self.msg_cd = msg_cd
        self.msg1 = msg1
        super().__init__(f"[{msg_cd}] {msg1}")


def load_config(path: str = "config.yaml") -> dict[str, Any]:
    """
    Load config.yaml and overlay KIS credentials from .env (or environment variables).

    .env variables take precedence over anything in config.yaml so that
    secrets are never stored in the YAML file.

    Required env vars:  KIS_APP_KEY, KIS_APP_SECRET
    Optional env vars:  KIS_ENV  (prod | paper, default: prod)
                        KIS_BASE_URL  (overrides base_url_prod when set)
    """
    from dotenv import load_dotenv  # lazy import — only used at startup

    # Load .env from the project root (same directory as config.yaml)
    env_path = Path(path).parent / ".env"
    load_dotenv(dotenv_path=env_path, override=False)

    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Overlay credentials from environment — fail fast if missing
    app_key = os.environ.get("KIS_APP_KEY")
    app_secret = os.environ.get("KIS_APP_SECRET")
    if not app_key or not app_secret:
        raise EnvironmentError(
            "KIS_APP_KEY and KIS_APP_SECRET must be set in .env or the environment."
        )
    config.setdefault("kis", {})["app_key"] = app_key
    config["kis"]["app_secret"] = app_secret

    if kis_env := os.environ.get("KIS_ENV"):
        config["kis"]["env"] = kis_env
    if base_url := os.environ.get("KIS_BASE_URL"):
        env_key = f"base_url_{config['kis'].get('env', 'prod')}"
        config["kis"][env_key] = base_url

    return config


def _base_url(config: dict) -> str:
    env = config["kis"]["env"]
    key = f"base_url_{env}"
    return config["kis"][key]


def _token_cache_path(config: dict) -> Path:
    cache_dir = Path(config["output"]["cache_dir"])
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "token.json"


def issue_token(app_key: str, app_secret: str, base_url: str) -> dict[str, Any]:
    """Issue a new access token from KIS and return the raw response body."""
    url = f"{base_url}/oauth2/tokenP"
    payload = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret": app_secret,
    }
    resp = httpx.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data


def get_token(config: dict) -> str:
    """
    Return a valid access token.
    Loads from cache if still valid, otherwise issues a new one.
    """
    cache_path = _token_cache_path(config)
    app_key = config["kis"]["app_key"]
    app_secret = config["kis"]["app_secret"]
    base_url = _base_url(config)

    # Try cache
    if cache_path.exists():
        with open(cache_path, encoding="utf-8") as f:
            cached = json.load(f)
        issued_at = datetime.fromisoformat(cached["issued_at"])
        elapsed = (datetime.now(timezone.utc) - issued_at).total_seconds()
        if elapsed < cached["expires_in"] - 300:  # 5-min buffer
            return cached["access_token"]

    # Issue new token
    data = issue_token(app_key, app_secret, base_url)
    cache = {
        "access_token": data["access_token"],
        "issued_at": datetime.now(timezone.utc).isoformat(),
        "expires_in": data.get("expires_in", 86400),
    }
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    return cache["access_token"]


def build_headers(token: str, tr_id: str, config: dict) -> dict[str, str]:
    """Build standard request headers for KIS REST API calls."""
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": config["kis"]["app_key"],
        "appsecret": config["kis"]["app_secret"],
        "tr_id": tr_id,
        "custtype": "P",
    }
