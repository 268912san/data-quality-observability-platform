"""Cloud entry point: only the bundled Gold aggregates are accessible to the app."""

from pathlib import Path

import streamlit as st

from silver_dashboard import render


st.set_page_config(page_title="NYC Taxi | Data Quality", page_icon=":taxi:", layout="wide")
render(Path(__file__).resolve().parent / "gold")
