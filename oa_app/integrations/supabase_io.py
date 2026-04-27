"""Supabase integration helpers."""

from __future__ import annotations

import random
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, TypeVar

import streamlit as st

try:
    import tomllib
except Exception:  # pragma: no cover
    tomllib = None

T = TypeVar("T")
_FALLBACK_SECRETS_PATH = Path(r"C:\OA-Scheduling-Assistant\.streamlit\secrets.toml")


def _local_secret(name: str) -> str:
    try:
        value = st.secrets.get(name)
    except Exception:
        value = None
    return str(value or "").strip()


@lru_cache(maxsize=1)
def _fallback_supabase_values() -> tuple[str, str]:
    path = _FALLBACK_SECRETS_PATH
    if not tomllib or not path.exists():
        return "", ""
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return "", ""
    return (
        str(data.get("SUPABASE_URL") or "").strip(),
        str(data.get("SUPABASE_KEY") or "").strip(),
    )


def _supabase_credentials() -> tuple[str, str]:
    url = _local_secret("SUPABASE_URL")
    key = _local_secret("SUPABASE_KEY")
    if url and key:
        return url, key
    fb_url, fb_key = _fallback_supabase_values()
    return fb_url, fb_key


def supabase_enabled() -> bool:
    url, key = _supabase_credentials()
    return bool(url and key)


@st.cache_resource(show_spinner=False)
def get_supabase():
    from supabase import create_client

    url, key = _supabase_credentials()
    if not (url and key):
        raise RuntimeError("Supabase secrets missing: SUPABASE_URL / SUPABASE_KEY")
    return create_client(url, key)


def with_retry(fn: Callable[..., T], *args: Any, retries: int = 5, base: float = 0.35, **kwargs: Any) -> T:
    last: Exception | None = None
    for i in range(max(1, retries)):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # pragma: no cover
            last = exc
            time.sleep(base * (2**i) + random.random() * 0.15)
    assert last is not None
    raise last
