# dashboard/pages/2_Macro_Environment.py

import streamlit as st
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Macro Environment", page_icon="🏦", layout="wide")

@st.cache_resource
def get_engine():
    db_url = (
        f"postgresql://{os.environ.get('POSTGRES_USER')}:"
        f"{os.environ.get('POSTGRES_PASSWORD')}@"
        f"{os.environ.get('POSTGRES_HOST', 'localhost')}:"
        f"{os.environ.get('POSTGRES_PORT', '5432')}/"
        f"{os.environ.get('POSTGRES_DB', 'lakehouse')}"
    )
    return create_engine(db_url)

@st.cache_data(ttl=300)
def load_macro():
    engine = get_engine()
    return pd.read_sql("SELECT * FROM mart_macro_dashboard", engine)

@st.cache_data(ttl=300)
def load_fx():
    engine = get_engine()
    return pd.read_sql("SELECT * FROM fact_fx_rates ORDER BY rate_date DESC", engine)

macro_df = load_macro()
fx_df = load_fx()

st.title("🏦 UK Macro Environment")
st.caption("Bank of England & ONS data")

# ── Key macro metrics ─────────────────────────────────────────────────────────
st.subheader("Key Economic Indicators")

col1, col2, col3, col4 = st.columns(4)

def get_indicator(df, name):
    row = df[df["indicator"] == name]
    if row.empty:
        return None, None
    return row.iloc[0]["value"], row.iloc[0]["period"]

base_rate, br_period = get_indicator(macro_df, "base_rate")
inflation, inf_period = get_indicator(macro_df, "inflation_cpi")
gdp, gdp_period = get_indicator(macro_df, "gdp_growth")
unemployment, u_period = get_indicator(macro_df, "unemployment_rate")

with col1:
    st.metric(
        "BOE Base Rate",
        f"{base_rate}%" if base_rate else "N/A",
        help=f"Period: {br_period}"
    )
with col2:
    st.metric(
        "CPI Inflation",
        f"{inflation}%" if inflation else "N/A",
        help=f"Period: {inf_period}"
    )
with col3:
    st.metric(
        "GDP Growth",
        f"{gdp}%" if gdp else "N/A",
        help=f"Period: {gdp_period}"
    )
with col4:
    st.metric(
        "Unemployment",
        f"{unemployment}%" if unemployment else "N/A",
        help=f"Period: {u_period}"
    )

st.divider()

# ── Macro indicators table ────────────────────────────────────────────────────
st.subheader("All Macro Indicators")
display = macro_df[["display_name", "value", "period", "source_system"]].copy()
display.columns = ["Indicator", "Value", "Period", "Source"]
st.dataframe(display, use_container_width=True)

st.divider()

# ── FX rates ──────────────────────────────────────────────────────────────────
st.subheader("GBP Exchange Rates")

if not fx_df.empty:
    col1, col2 = st.columns(2)
    for i, (_, row) in enumerate(fx_df.iterrows()):
        col = col1 if i % 2 == 0 else col2
        with col:
            st.metric(
                row["currency_pair"],
                f"{row['exchange_rate']:.4f}",
                help=f"Last refreshed: {row['last_refreshed']}"
            )
else:
    st.warning("No FX rate data available")
