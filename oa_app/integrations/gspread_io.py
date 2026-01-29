"""Google Sheets (gspread) integration helpers.

Centralizes:
- Service-account auth (via Streamlit secrets)
- Retry/backoff for quota / transient errors
- Opening spreadsheets by URL

This keeps UI and business logic modules free of gspread boilerplate.
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
    """Run a gspread operation with exponential backoff on transient errors."""
    base = 0.6
    for i in range(6):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            status_code = getattr(getattr(e, "response", None), "status_code", None)

            # gspread APIError with transient HTTP codes
            if isinstance(e, APIError) and status_code in (429, 500, 502, 503, 504):
                if i == 5:
                    raise
                time.sleep(base * (2**i) + random.uniform(0, 0.4))
                continue

            # Some quota errors surface as generic exceptions
            s = str(e).lower()
            if ("429" in s or "quota exceeded" in s) and i < 5:
                time.sleep(base * (2**i) + random.uniform(0, 0.4))
                continue

            raise


@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    creds_dict = dict(st.secrets.get("gcp_service_account", {}))  # type: ignore[arg-type]
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
