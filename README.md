# OA Scheduler — Sprint 1 (Pruned from end-goal)

This repo is a **trimmed version of the end-goal zip**, keeping ONLY:

- Streamlit app shell
- Config + Streamlit secrets template
- **[FG][Sprint 1]** Google Sheets connection
- Load roster + validate typed OA name
- Peek sheet **exactly as in sheet** (same Peek UI from end-goal)

## Run

1. Copy `.streamlit/secrets.example.toml` → `.streamlit/secrets.toml` and fill it.
2. Install requirements: `pip install -r requirements.txt`
3. Start: `streamlit run app.py`
