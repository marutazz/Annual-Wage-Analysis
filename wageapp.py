import os
from pathlib import Path
import tempfile
import streamlit as st
from cleaning_module import clean_and_engineer, append_to_database

st.set_page_config(page_title="Ministry of Welfare | Wage Data Upload", layout="centered")

LOGO_FILE = "ministry_logo.png"
APP_DIR = Path(__file__).parent
LOGO_PATH = APP_DIR / LOGO_FILE

c1, c2, c3 = st.columns([1, 2, 1])
with c2:
    if LOGO_PATH.exists():
        st.image(str(LOGO_PATH), width=260)
    else:
        st.warning(f"Logo not found at {LOGO_PATH}")

st.markdown("<h1 style='text-align: center;'>Ministry of Welfare</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center;'>Insert or upload the annual wage data</p>", unsafe_allow_html=True)
uploaded_file = st.file_uploader(
    label="",
    type=["xlsx", "xls", "csv"],
    label_visibility="collapsed",
    help="Limit 200MB per file • XLSX, XLS, CSV"
)

SERVER = r"Maruta\MSSQLSERVER02"
DATABASE = "WageData"
TABLE = "RegionalWageData"

if uploaded_file is not None:
    try:
        suffix = os.path.splitext(uploaded_file.name)[1] or ".xlsx"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name

        with st.spinner("Cleaning and appending data…"):
            df_clean = clean_and_engineer(tmp_path)
            append_to_database(df_clean, SERVER, DATABASE, TABLE)

        st.success("Data successfully cleaned and added to the database.")
        with st.expander("Preview cleaned data"):
            st.dataframe(df_clean.head())

    except Exception as e:
        st.error(str(e))
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
