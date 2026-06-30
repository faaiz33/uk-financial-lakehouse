# dashboard/pages/3_Pipeline_Health.py

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
import subprocess
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Pipeline Health", page_icon="⚙️", layout="wide")

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

@st.cache_data(ttl=60)
def get_table_stats():
    engine = get_engine()
    query = """
        SELECT 'raw_ftse_prices' as table_name,
               COUNT(*) as row_count,
               MAX(created_at) as last_updated
        FROM raw_ftse_prices
        UNION ALL
        SELECT 'raw_fx_rates',
               COUNT(*),
               MAX(created_at)
        FROM raw_fx_rates
        UNION ALL
        SELECT 'raw_macro_indicators',
               COUNT(*),
               MAX(created_at)
        FROM raw_macro_indicators
        UNION ALL
        SELECT 'fact_daily_prices',
               COUNT(*),
               MAX(ingested_at)
        FROM fact_daily_prices
        UNION ALL
        SELECT 'fact_fx_rates',
               COUNT(*),
               MAX(ingested_at)
        FROM fact_fx_rates
        UNION ALL
        SELECT 'mart_macro_dashboard',
               COUNT(*),
               MAX(ingested_at)
        FROM mart_macro_dashboard
    """
    return pd.read_sql(query, engine)

st.title("⚙️ Pipeline Health")
st.caption("Last 60 seconds cached | Click refresh to update")

if st.button("🔄 Refresh"):
    st.cache_data.clear()
    st.rerun()

st.divider()

# ── Table status ──────────────────────────────────────────────────────────────
st.subheader("Table Status")

try:
    stats = get_table_stats()
    now = datetime.now(timezone.utc)

    def freshness_indicator(last_updated):
        if pd.isna(last_updated):
            return "🔴 No data"
        if hasattr(last_updated, 'tzinfo') and last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=timezone.utc)
        hours_ago = (now - last_updated).total_seconds() / 3600
        if hours_ago < 25:
            return f"🟢 {hours_ago:.1f}h ago"
        elif hours_ago < 49:
            return f"🟡 {hours_ago:.1f}h ago"
        else:
            return f"🔴 {hours_ago:.1f}h ago"

    stats["Freshness"] = stats["last_updated"].apply(freshness_indicator)
    stats.columns = ["Table", "Row Count", "Last Updated", "Freshness"]
    st.dataframe(stats, use_container_width=True)

except Exception as e:
    st.error(f"Could not load table stats: {e}")

st.divider()

# ── dbt status ────────────────────────────────────────────────────────────────
st.subheader("dbt Models")

project_root = "/Users/mohammedaminulfaaiz/uk-financial-lakehouse"
dbt_bin = f"{project_root}/venv/bin/dbt"

if st.button("Run dbt ls"):
    with st.spinner("Running dbt ls..."):
        result = subprocess.run(
            [dbt_bin, "ls", "--select", "bronze silver gold"],
            capture_output=True,
            text=True,
            cwd=f"{project_root}/dbt"
        )
        if result.returncode == 0:
            models = [l for l in result.stdout.split("\n") if l.strip()]
            st.success(f"✅ {len(models)} dbt models available")
            for model in models:
                st.code(model)
        else:
            st.error(f"dbt error: {result.stderr}")

st.divider()

# ── Data quality status ───────────────────────────────────────────────────────
st.subheader("Data Quality")

if st.button("Run GX Checkpoint"):
    with st.spinner("Running Great Expectations checks..."):
        result = subprocess.run(
            [f"{project_root}/venv/bin/python",
             f"{project_root}/great_expectations/run_checkpoint.py"],
            capture_output=True,
            text=True,
            cwd=project_root
        )
        if result.returncode == 0:
            st.success("✅ All data quality checks passed")
        else:
            st.error("❌ Data quality checks failed")
        st.code(result.stdout)

st.divider()
st.caption("UK Financial Markets Lakehouse Pipeline — Built with Kafka, dbt, Airflow, Great Expectations")
