"""Google Sheets (gspread) helpers.

Centralizes Streamlit secrets auth + basic retry/backoff for quota bursts.
"""

from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

import gspread
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

T = TypeVar("T")


def with_backoff(fn: Callable[..., T], *args, **kwargs) -> T:
    """Retry gspread calls on 429/5xx with exponential backoff + jitter."""
    base = 0.6
    for i in range(6):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            sc = getattr(getattr(e, "response", None), "status_code", None)
            transient = isinstance(e, APIError) and sc in (429, 500, 502, 503, 504)
            s = str(e).lower()
            textual_quota = ("429" in s) or ("quota exceeded" in s)
            if transient or textual_quota:
                if i == 5:
                    raise
                time.sleep(base * (2 ** i) + random.uniform(0, 0.4))
                continue
            raise


def retry_429(fn: Callable[..., T], *args, retries: int = 5, backoff: float = 0.8, **kwargs) -> T:
    """Generic retry helper for 429 bursts on non-batch gspread ops."""
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            s = str(e).lower()
            if "429" in s or "quota exceeded" in s:
                time.sleep(backoff * (2 ** i))
                continue
            raise
    return fn(*args, **kwargs)


@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    creds_dict = dict(st.secrets.get("gcp_service_account", {}))  # type: ignore
    if not creds_dict:
        st.error("Missing service account in secrets (gcp_service_account).")
        st.stop()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


@st.cache_resource(show_spinner=False)
def open_spreadsheet(spreadsheet_url: str) -> gspread.Spreadsheet:
    client = get_gspread_client()
    return with_backoff(client.open_by_url, spreadsheet_url)
