# dashboard/app.py
# Main entry point — Market Overview page

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="UK Financial Markets Lakehouse",
    page_icon="🇬🇧",
    layout="wide",
)

# ── Database connection ───────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    """
    SQLAlchemy engine — cached so we reuse the connection pool.
    @st.cache_resource means this only runs once per session,
    not on every page refresh.
    """
    db_url = (
        f"postgresql://{os.environ.get('POSTGRES_USER')}:"
        f"{os.environ.get('POSTGRES_PASSWORD')}@"
        f"{os.environ.get('POSTGRES_HOST', 'localhost')}:"
        f"{os.environ.get('POSTGRES_PORT', '5432')}/"
        f"{os.environ.get('POSTGRES_DB', 'lakehouse')}"
    )
    return create_engine(db_url)

@st.cache_data(ttl=300)  # cache for 5 minutes
def load_fact_daily_prices():
    engine = get_engine()
    return pd.read_sql("SELECT * FROM fact_daily_prices ORDER BY trade_date DESC", engine)

@st.cache_data(ttl=300)
def load_sector_performance():
    engine = get_engine()
    return pd.read_sql("SELECT * FROM mart_sector_performance ORDER BY trade_date DESC", engine)

# ── Load data ─────────────────────────────────────────────────────────────────
df = load_fact_daily_prices()
sector_df = load_sector_performance()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🇬🇧 UK Financial Markets Lakehouse")
st.caption("Data refreshed daily | Source: Yahoo Finance, ONS, Bank of England")

# ── Summary metrics ───────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

latest_date = df["trade_date"].max()
latest = df[df["trade_date"] == latest_date]

stocks_up = len(latest[latest["daily_return_pct"] > 0])
stocks_down = len(latest[latest["daily_return_pct"] < 0])
avg_return = latest["daily_return_pct"].mean()
total_volume = latest["volume"].sum()

with col1:
    st.metric("Stocks Up", stocks_up, delta=None)
with col2:
    st.metric("Stocks Down", stocks_down, delta=None)
with col3:
    st.metric("Avg Return", f"{avg_return:.4f}%")
with col4:
    st.metric("Total Volume", f"{total_volume:,.0f}")

st.divider()

# ── FTSE heatmap ──────────────────────────────────────────────────────────────
st.subheader("FTSE 100 — Daily Returns Heatmap")

fig = px.treemap(
    latest,
    path=["sector", "ticker"],
    values="volume",
    color="daily_return_pct",
    color_continuous_scale=["red", "white", "green"],
    color_continuous_midpoint=0,
    hover_data={"close_price": True, "daily_return_pct": ":.4f"},
    title="Size = Volume | Colour = Daily Return %"
)
fig.update_layout(height=500)
st.plotly_chart(fig, use_container_width=True)

# ── Top movers ────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("🟢 Top 5 Gainers")
    gainers = latest.nlargest(5, "daily_return_pct")[
        ["ticker", "sector", "close_price", "daily_return_pct"]
    ].reset_index(drop=True)
    gainers.columns = ["Ticker", "Sector", "Close (p)", "Return %"]
    st.dataframe(gainers, use_container_width=True)

with col2:
    st.subheader("🔴 Top 5 Losers")
    losers = latest.nsmallest(5, "daily_return_pct")[
        ["ticker", "sector", "close_price", "daily_return_pct"]
    ].reset_index(drop=True)
    losers.columns = ["Ticker", "Sector", "Close (p)", "Return %"]
    st.dataframe(losers, use_container_width=True)

# ── Volume leaders ────────────────────────────────────────────────────────────
st.subheader("📊 Volume Leaders")
volume_leaders = latest.nlargest(10, "volume")[
    ["ticker", "sector", "volume", "close_price", "daily_return_pct"]
].reset_index(drop=True)

fig_vol = px.bar(
    volume_leaders,
    x="ticker",
    y="volume",
    color="daily_return_pct",
    color_continuous_scale=["red", "white", "green"],
    color_continuous_midpoint=0,
    title="Top 10 Stocks by Volume",
    labels={"ticker": "Stock", "volume": "Volume", "daily_return_pct": "Return %"}
)
st.plotly_chart(fig_vol, use_container_width=True)

st.caption(f"Data as of: {latest_date} | Pipeline: UK Financial Markets Lakehouse")
