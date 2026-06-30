# 🇬🇧 UK Financial Markets Lakehouse Pipeline

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python)
![dbt](https://img.shields.io/badge/dbt-1.9.8-orange?logo=dbt)
![Airflow](https://img.shields.io/badge/Airflow-3.2.2-017CEE?logo=apache-airflow)
![Kafka](https://img.shields.io/badge/Kafka-7.4.0-231F20?logo=apache-kafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![GitHub Actions](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?logo=github-actions)
![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?logo=streamlit)

> **Production-grade, end-to-end data engineering pipeline** ingesting real UK financial and economic data — from live APIs through Kafka streaming, dbt medallion transformations, Airflow orchestration, and Great Expectations data quality checks — served to two live dashboards.

---

## 📐 Architecture

    DATA SOURCES
    Yahoo Finance · ONS API · Bank of England
            │
            ▼
    INGESTION LAYER
    Python Producers → Apache Kafka
    Topics: ftse_prices · fx_rates · macro_indicators
            │
            ▼
    BRONZE LAYER (Raw)
    Kafka Consumers → PostgreSQL
    raw_ftse_prices · raw_fx_rates · raw_macro_indicators
            │
            ▼
    SILVER LAYER (Cleaned)
    dbt models — Deduplication · Type casting · Derived columns
            │
            ▼
    GOLD LAYER (Business-ready)
    dbt models
    fact_daily_prices · fact_fx_rates
    mart_sector_performance · mart_macro_dashboard
            │
      ┌─────┴─────┐
      ▼           ▼
    STREAMLIT   METABASE
    :8501       :3000
    Engineering  Business BI

    ─────────────────────────────────────
    ORCHESTRATION: Apache Airflow 3.2.2
    DAGs: ingest_market_data · ingest_macro_data · run_dbt_pipeline
    ─────────────────────────────────────
    DATA QUALITY: Great Expectations
    13 automated checks across 3 gold tables
    ─────────────────────────────────────
    CI/CD: GitHub Actions
    dbt test on every push to main
    ─────────────────────────────────────

---

## 📊 Data Sources

| Source | Data | Cost |
|--------|------|------|
| 🟡 Yahoo Finance | FTSE 100 prices — 20 stocks, 7 sectors, OHLCV | Free |
| 🟡 Yahoo Finance | GBP/USD · GBP/EUR exchange rates | Free |
| 🟢 ONS API | GDP growth · Unemployment · CPI inflation | Free |
| 🟢 Bank of England | UK base interest rate | Free |

---

## 🛠️ Tech Stack

| Layer | Tool | Why This Tool |
|-------|------|---------------|
| 🔴 Streaming | Apache Kafka 7.4.0 | Decouples producers from consumers. Buffers data if DB goes down. Industry standard at every major bank. |
| 🐘 Storage | PostgreSQL 15 | Reliable, widely used relational DB. Runs in Docker. |
| 🟠 Transformation | dbt Core 1.9.8 | Version-controlled SQL. Dependency management. Built-in testing. Used by Revolut, Monzo. |
| 🌀 Orchestration | Apache Airflow 3.2.2 | Industry standard scheduler. DAGs, retries, alerting. |
| ✅ Data Quality | Great Expectations 1.18.2 | Business-rule validation beyond what dbt tests cover. |
| 🔴 Dashboard | Streamlit 1.58.0 | Python dashboards for engineering demos. |
| 🔵 BI Tool | Metabase 0.47.7 | Drag-and-drop BI for business stakeholder demos. |
| ⚙️ CI/CD | GitHub Actions | Automated dbt tests on every pull request. |
| 🐳 Container | Docker + Compose | Reproducible local environment. |

---

## 🗂️ Project Structure

    uk-financial-lakehouse/
    │
    ├── ingestion/
    │   ├── producers/           # Fetch from APIs → publish to Kafka
    │   │   ├── ftse_producer.py
    │   │   ├── fx_producer.py
    │   │   └── macro_producer.py
    │   └── consumers/           # Read from Kafka → write to PostgreSQL
    │       ├── ftse_consumer.py
    │       ├── fx_consumer.py
    │       └── macro_consumer.py
    │
    ├── dbt/
    │   └── models/
    │       ├── bronze/          # Raw views over source tables
    │       ├── silver/          # Cleaned, typed, derived columns
    │       └── gold/            # Business-ready facts and marts
    │
    ├── airflow/
    │   └── dags/
    │       ├── ingest_market_data.py    # 8am Mon-Fri
    │       ├── ingest_macro_data.py     # 9am Mon-Fri
    │       └── run_dbt_pipeline.py      # 10am Mon-Fri
    │
    ├── great_expectations/      # 13 automated data quality checks
    │   └── run_checkpoint.py
    │
    ├── dashboard/               # Streamlit — 4 pages
    │   ├── app.py               # Market Overview
    │   └── pages/
    │       ├── 1_Sector_Performance.py
    │       ├── 2_Macro_Environment.py
    │       └── 3_Pipeline_Health.py
    │
    ├── docker-compose.yml       # Kafka + Zookeeper + PostgreSQL + Metabase
    └── .github/workflows/
        └── dbt_ci.yml           # GitHub Actions CI

---

## 📈 Medallion Architecture

| Layer | Materialisation | Tables | Purpose |
|-------|----------------|--------|---------|
| 🥉 **Bronze** | View | `bronze_ftse_prices` `bronze_fx_rates` `bronze_macro_indicators` | Exact copy of raw source. Never modified. Reprocess from here if anything breaks. |
| 🥈 **Silver** | View | `silver_ftse_prices` `silver_fx_rates` `silver_macro_indicators` | Cleaned, deduplicated, type-cast. Adds `daily_return_pct` and other derived columns. |
| 🥇 **Gold** | Table | `fact_daily_prices` `fact_fx_rates` `mart_sector_performance` `mart_macro_dashboard` | Business-ready aggregations. What dashboards query directly. |

---

## ✅ Data Quality — 13 Automated Checks

**fact_daily_prices**
- ticker → not null
- close_price → not null · between 0 and 1,000,000
- volume → between 0 and 1,000,000,000
- daily_return_pct → between -50% and +50%
- sector → in [Financials, Energy, Healthcare, Consumer, Technology, Materials, Utilities]

**fact_fx_rates**
- currency_pair → not null
- exchange_rate → not null · between 0.1 and 10
- from_currency → must be GBP

**mart_macro_dashboard**
- indicator → not null
- value → not null · between -20 and 100

Exit code 0 → all pass → Airflow task succeeds
Exit code 1 → any fail → Airflow task fails → retry → alert

---

## 🔄 Airflow DAGs

**08:00 Mon-Fri — ingest_market_data**
ftse_producer → ftse_consumer | fx_producer → fx_consumer

**09:00 Mon-Fri — ingest_macro_data**
macro_producer → macro_consumer

**10:00 Mon-Fri — run_dbt_pipeline**
dbt bronze → dbt silver → dbt gold → Great Expectations checkpoint

---

## 💬 Key Design Decisions

**Why Kafka and not direct database writes?**
If PostgreSQL goes down, Kafka buffers messages on disk. When it recovers, consumers replay from the last offset. Nothing is lost. Direct writes lose data permanently during outages.

**Why medallion architecture?**
Bronze is immutable — if transformation logic has a bug, reprocess from bronze without re-fetching from APIs. Silver cleans once. Gold is optimised for query performance. Each layer has one job.

**Why dbt and not raw SQL scripts?**
dbt manages execution order automatically via dependency graphs. Every model is version-controlled, testable, and self-documenting. Raw scripts require manual order management and have no built-in testing.

**Why Great Expectations alongside dbt tests?**
dbt tests cover structural integrity — not null, uniqueness. GX covers business rules — value ranges, categorical membership. Together they catch both schema bugs and data quality regressions.

---

## 🏦 Real World Context

| Tool | Where It's Used |
|------|----------------|
| Kafka | JPMorgan, Goldman Sachs, Barclays — every major investment bank |
| dbt | Revolut, Monzo, Airbnb, GitLab |
| Medallion architecture | Databricks, Delta Lake, most modern lakehouses |
| Airflow | Uber, Airbnb — the original authors |
| Great Expectations | Data contracts in regulated financial services |

---

## 🚀 Quick Start

    git clone https://github.com/faaiz33/uk-financial-lakehouse.git
    cd uk-financial-lakehouse
    python -m venv venv && source venv/bin/activate
    pip install -r ingestion/requirements.txt
    docker-compose up -d
    python ingestion/producers/ftse_producer.py
    python ingestion/consumers/ftse_consumer.py
    cd dbt && dbt run && dbt test
    streamlit run dashboard/app.py

---

*🇬🇧 Built with real UK market data · Python 3.13 · dbt · Airflow · Kafka · PostgreSQL · Great Expectations*