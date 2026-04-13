"""Visual-only theme layer shared with the original OA Scheduling Assistant."""

from __future__ import annotations

import streamlit as st


_THEME_CSS = r"""
<style>
  :root {
    --oa-accent: #4F46E5;
    --oa-accent2: #14B8A6;
    --oa-ink: rgba(15, 23, 42, 0.92);
    --oa-ink-muted: rgba(15, 23, 42, 0.62);
    --oa-surface: rgba(238, 242, 255, 0.72);
    --oa-surface-strong: rgba(238, 242, 255, 0.92);
    --oa-border: rgba(15, 23, 42, 0.08);
    --oa-radius: 14px;
    --oa-radius-card: 12px;
    --oa-radius-control: 10px;
    --oa-shadow: 0 18px 46px rgba(2, 6, 23, 0.10);
    --oa-shadow-soft: 0 10px 26px rgba(2, 6, 23, 0.07);
  }

  html,
  body,
  .stApp,
  [data-testid="stAppViewContainer"] {
    background:
      radial-gradient(900px 500px at 12% 8%, rgba(79, 70, 229, 0.12), rgba(0, 0, 0, 0)),
      radial-gradient(900px 520px at 88% 12%, rgba(20, 184, 166, 0.10), rgba(0, 0, 0, 0)),
      linear-gradient(180deg, #F6F7FB 0%, #EEF2FF 60%, #F8FAFC 100%);
  }

  [data-testid="stHeader"],
  [data-testid="stToolbar"],
  [data-testid="stSidebar"],
  [data-testid="stMain"],
  section[data-testid="stMain"] {
    background: transparent;
  }

  [data-testid="stDecoration"] {
    background-image: linear-gradient(90deg, var(--oa-accent), var(--oa-accent2));
    opacity: 0.95;
  }

  [data-testid="stAppViewContainer"] .main .block-container,
  [data-testid="stMainBlockContainer"],
  section[data-testid="stMain"] .block-container {
    margin-top: 0.85rem;
    background: linear-gradient(180deg, rgba(238, 242, 255, 0.70), rgba(224, 231, 255, 0.54));
    border: 1px solid var(--oa-border);
    border-radius: var(--oa-radius);
    box-shadow: var(--oa-shadow);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    padding: 2rem;
  }

  [data-testid="stSidebar"] > div:first-child {
    background: linear-gradient(180deg, rgba(224, 231, 255, 0.94), rgba(238, 242, 255, 0.86));
    border-right: 1px solid var(--oa-border);
  }

  h1,
  h2,
  h3,
  h4,
  label,
  p,
  li,
  .stMarkdown,
  .stCaption,
  small,
  [data-testid="stMarkdownContainer"],
  [data-testid="stCaptionContainer"] {
    color: var(--oa-ink) !important;
  }

  .stCaption,
  small,
  [data-testid="stCaptionContainer"] {
    color: var(--oa-ink-muted) !important;
  }

  .stTextInput input,
  .stTextArea textarea,
  .stNumberInput input,
  .stDateInput input,
  .stTimeInput input,
  [data-testid="stChatInput"] textarea,
  [data-testid="stSelectbox"] div[data-baseweb="select"] > div,
  [data-testid="stMultiSelect"] div[data-baseweb="select"] > div {
    background: var(--oa-surface-strong) !important;
    border-radius: var(--oa-radius-control) !important;
    border: 1px solid rgba(15, 23, 42, 0.10) !important;
    box-shadow: 0 10px 22px rgba(2, 6, 23, 0.05);
  }

  .stTextInput input:focus,
  .stTextArea textarea:focus,
  .stNumberInput input:focus,
  .stDateInput input:focus,
  .stTimeInput input:focus,
  [data-testid="stChatInput"] textarea:focus {
    border-color: rgba(79, 70, 229, 0.38) !important;
    box-shadow: 0 0 0 3px rgba(79, 70, 229, 0.12) !important;
  }

  [data-testid="stSelectbox"] label,
  [data-testid="stMultiSelect"] label,
  [data-testid="stSelectbox"] [data-testid="stMarkdownContainer"],
  [data-testid="stMultiSelect"] [data-testid="stMarkdownContainer"] {
    background: transparent !important;
    border: 0 !important;
    box-shadow: none !important;
    padding: 0 !important;
  }

  .stButton > button,
  [data-testid="stChatInputSubmitButton"] button {
    border: 0 !important;
    border-radius: 12px !important;
    font-weight: 700 !important;
    color: white !important;
    background: linear-gradient(135deg, rgba(79, 70, 229, 0.95), rgba(20, 184, 166, 0.90)) !important;
    box-shadow: 0 12px 28px rgba(2, 6, 23, 0.14);
    transition: transform 120ms ease, filter 120ms ease, box-shadow 120ms ease;
  }

  .stButton > button:hover,
  [data-testid="stChatInputSubmitButton"] button:hover {
    filter: brightness(1.03);
    transform: translateY(-1px);
    box-shadow: 0 16px 34px rgba(2, 6, 23, 0.16);
  }

  .stButton > button[kind="secondary"] {
    background: linear-gradient(180deg, rgba(238, 242, 255, 0.88), rgba(224, 231, 255, 0.78)) !important;
    color: var(--oa-ink) !important;
    border: 1px solid rgba(15, 23, 42, 0.12) !important;
    box-shadow: 0 10px 22px rgba(2, 6, 23, 0.06);
  }

  [data-testid="stMetric"],
  [data-testid="stAlert"],
  [data-testid="stDataFrame"],
  [data-testid="stPlotlyChart"],
  [data-testid="stTable"],
  [data-testid="stChatMessage"],
  [data-testid="stExpander"] details {
    background: linear-gradient(180deg, rgba(238, 242, 255, 0.70), rgba(224, 231, 255, 0.55)) !important;
    border: 1px solid rgba(15, 23, 42, 0.08) !important;
    border-radius: var(--oa-radius-card) !important;
    box-shadow: var(--oa-shadow-soft) !important;
  }

  [data-testid="stChatMessage"] {
    padding: 0.9rem 1rem;
  }

  [data-testid="stChatInput"] {
    background: rgba(238, 242, 255, 0.84) !important;
    border: 1px solid rgba(15, 23, 42, 0.08) !important;
    border-radius: 14px !important;
    box-shadow: 0 10px 26px rgba(2, 6, 23, 0.08);
  }

  [data-testid="stExpander"] details > summary {
    border-radius: 12px !important;
    padding: 0.5rem 0.65rem !important;
    background: linear-gradient(90deg, rgba(79, 70, 229, 0.10), rgba(20, 184, 166, 0.08)) !important;
    border: 1px solid rgba(15, 23, 42, 0.08) !important;
  }

  [data-testid="stDataFrame"] [role="grid"],
  [data-testid="stDataFrame"] [role="row"],
  [data-testid="stDataFrame"] [role="gridcell"],
  [data-testid="stPlotlyChart"] > div,
  [data-testid="stVerticalBlock"],
  [data-testid="stHorizontalBlock"],
  [data-testid="stContainer"],
  [data-testid="stBlock"] {
    background: transparent !important;
  }

  .stProgress > div > div > div > div {
    background: linear-gradient(90deg, rgba(79, 70, 229, 0.95), rgba(20, 184, 166, 0.90));
  }

  .stProgress > div > div > div {
    background: rgba(15, 23, 42, 0.08);
  }
</style>
"""


def apply_vibrant_theme() -> None:
    """Inject the shared OA visual theme on every rerun."""
    st.markdown(_THEME_CSS, unsafe_allow_html=True)
