# dashboard/pages/1_Sector_Performance.py

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Sector Performance", page_icon="📊", layout="wide")

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
def load_sector_performance():
    engine = get_engine()
    return pd.read_sql("SELECT * FROM mart_sector_performance ORDER BY trade_date DESC", engine)

@st.cache_data(ttl=300)
def load_fact_daily_prices():
    engine = get_engine()
    return pd.read_sql("SELECT * FROM fact_daily_prices ORDER BY trade_date DESC", engine)

sector_df = load_sector_performance()
prices_df = load_fact_daily_prices()

st.title("📊 Sector Performance")
st.caption("FTSE 100 returns broken down by sector")

latest_date = sector_df["trade_date"].max()
latest_sector = sector_df[sector_df["trade_date"] == latest_date].sort_values(
    "avg_return_pct", ascending=True
)

# ── Sector returns bar chart ──────────────────────────────────────────────────
st.subheader("Average Return by Sector")

fig = px.bar(
    latest_sector,
    x="avg_return_pct",
    y="sector",
    orientation="h",
    color="avg_return_pct",
    color_continuous_scale=["red", "white", "green"],
    color_continuous_midpoint=0,
    labels={"avg_return_pct": "Avg Return %", "sector": "Sector"},
    title=f"Sector Performance — {latest_date}",
    text="avg_return_pct",
)
fig.update_traces(texttemplate="%{text:.4f}%", textposition="outside")
fig.update_layout(height=400, showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# ── Stocks up vs down ─────────────────────────────────────────────────────────
st.subheader("Stocks Up vs Down by Sector")

fig2 = go.Figure()
fig2.add_trace(go.Bar(
    name="Up",
    x=latest_sector["sector"],
    y=latest_sector["stocks_up"],
    marker_color="green"
))
fig2.add_trace(go.Bar(
    name="Down",
    x=latest_sector["sector"],
    y=latest_sector["stocks_down"],
    marker_color="red"
))
fig2.update_layout(barmode="group", title="Breadth by Sector", height=350)
st.plotly_chart(fig2, use_container_width=True)

# ── Sector detail table ───────────────────────────────────────────────────────
st.subheader("Sector Detail")
display_cols = ["sector", "avg_return_pct", "best_return_pct",
                "worst_return_pct", "total_volume", "stocks_up", "stocks_down"]
st.dataframe(
    latest_sector[display_cols].reset_index(drop=True),
    use_container_width=True
)
