import streamlit as st

st.set_page_config(page_title="OA Scheduler", page_icon="🗓️", layout="wide")

page = st.sidebar.radio("Navigation", ["Home", "Schedule"])

st.title("🗓️ OA Scheduler")

if page == "Home":
    st.write("Sprint 1 baseline app. Go to Schedule to connect Sheets + view your week.")
else:
    st.subheader("Schedule (Sprint 1)")
    st.info("Roster + weekly view will appear here once Google Sheets is connected.")
    # IMPORTANT: even if secrets missing, this page should still render.
    st.caption("If credentials are missing, you'll see a clear fix hint instead of a crash.")
