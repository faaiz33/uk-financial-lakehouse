# 🇬🇧 UK Financial Markets Lakehouse Pipeline

> A production-grade, end-to-end data engineering portfolio project ingesting real UK financial and economic data through a medallion lakehouse architecture — from live APIs to business dashboards.

---

## 🏗️ Architecture
---

## 📊 Data Sources

| Source | Data | Method |
|--------|------|--------|
| Yahoo Finance (yfinance) | FTSE 100 stock prices — OHLCV, 20 stocks across 7 sectors | Free Python library, no API key |
| Yahoo Finance (yfinance) | GBP/USD and GBP/EUR exchange rates | Free Python library, no API key |
| ONS API | UK GDP growth, unemployment rate, CPI inflation | Official UK government API, free |
| Bank of England | UK base interest rate | Official API with hardcoded fallback |

---

## 🛠️ Tech Stack

| Layer | Tool | Version |
|-------|------|---------|
| Streaming | Apache Kafka | Confluent 7.4.0 |
| Message broker | Apache Zookeeper | Confluent 7.4.0 |
| Storage | PostgreSQL | 15 |
| Transformation | dbt Core | 1.9.8 |
| Orchestration | Apache Airflow | 3.2.2 |
| Data Quality | Great Expectations | 1.18.2 |
| Dashboard (engineering) | Streamlit | 1.58.0 |
| Dashboard (business) | Metabase | 0.47.7 |
| CI/CD | GitHub Actions | — |
| Containerisation | Docker + Docker Compose | — |
| Language | Python | 3.13 |

---

## 🗂️ Project Structure
uk-financial-lakehouse/
├── ingestion/
│   ├── producers/          # Kafka producers — fetch from APIs
│   │   ├── ftse_producer.py
│   │   ├── fx_producer.py
│   │   └── macro_producer.py
│   └── consumers/          # Kafka consumers — write to PostgreSQL
│       ├── ftse_consumer.py
│       ├── fx_consumer.py
│       └── macro_consumer.py
├── dbt/
│   └── models/
│       ├── bronze/         # Raw views over source tables
│       ├── silver/         # Cleaned, typed, derived columns
│       └── gold/           # Business-ready facts and marts
├── airflow/
│   └── dags/              # Three scheduled DAGs
│       ├── ingest_market_data.py
│       ├── ingest_macro_data.py
│       └── run_dbt_pipeline.py
├── great_expectations/    # Data quality checks
│   └── run_checkpoint.py
├── dashboard/             # Streamlit dashboard
│   ├── app.py             # Market Overview (Page 1)
│   └── pages/
│       ├── 1_Sector_Performance.py
│       ├── 2_Macro_Environment.py
│       └── 3_Pipeline_Health.py
├── docker-compose.yml     # Kafka + Zookeeper + PostgreSQL + Metabase
└── .github/
└── workflows/
└── dbt_ci.yml     # GitHub Actions CI
---

## 🚀 Running the Project

### Prerequisites
- Docker Desktop
- Python 3.11+
- Git

### 1. Clone and set up

```bash
git clone https://github.com/faaiz33/uk-financial-lakehouse.git
cd uk-financial-lakehouse
python -m venv venv
source venv/bin/activate
pip install -r ingestion/requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Start infrastructure

```bash
docker-compose up -d
```

### 4. Run ingestion

```bash
python ingestion/producers/ftse_producer.py
python ingestion/producers/fx_producer.py
python ingestion/producers/macro_producer.py

python ingestion/consumers/ftse_consumer.py
python ingestion/consumers/fx_consumer.py
python ingestion/consumers/macro_consumer.py
```

### 5. Run dbt transformations

```bash
cd dbt
dbt run
dbt test
```

### 6. Start dashboards

```bash
# Streamlit
streamlit run dashboard/app.py

# Metabase already running via Docker at localhost:3000
```

### 7. Start Airflow

```bash
export AIRFLOW_HOME=/path/to/uk-financial-lakehouse/airflow
airflow standalone
```

---

## 📈 Medallion Architecture

| Layer | Tables | Purpose |
|-------|--------|---------|
| **Bronze** | `bronze_ftse_prices`, `bronze_fx_rates`, `bronze_macro_indicators` | Raw data, exact copy of source. Never modified. Source of truth for reprocessing. |
| **Silver** | `silver_ftse_prices`, `silver_fx_rates`, `silver_macro_indicators` | Cleaned, deduplicated, type-cast. Derived columns like `daily_return_pct` added. |
| **Gold** | `fact_daily_prices`, `fact_fx_rates`, `mart_sector_performance`, `mart_macro_dashboard` | Business-ready aggregations. What dashboards query directly. |

---

## ✅ Data Quality

13 automated Great Expectations checks run after every dbt pipeline:

| Table | Checks |
|-------|--------|
| `fact_daily_prices` | ticker not null · close_price not null · close_price between 0-1M · volume between 0-1B · daily_return_pct between -50% and +50% · sector in valid set |
| `fact_fx_rates` | currency_pair not null · exchange_rate not null · exchange_rate between 0.1-10 · from_currency = GBP |
| `mart_macro_dashboard` | indicator not null · value not null · value between -20 and 100 |

---

## 🔄 Airflow DAGs

| DAG | Schedule | Tasks |
|-----|----------|-------|
| `ingest_market_data` | 08:00 Mon-Fri | FTSE producer → FTSE consumer · FX producer → FX consumer |
| `ingest_macro_data` | 09:00 Mon-Fri | Macro producer → Macro consumer |
| `run_dbt_pipeline` | 10:00 Mon-Fri | dbt bronze → dbt silver → dbt gold → GX checkpoint |

---

## 💬 Key Design Decisions

**Why Kafka and not direct database writes?**
Decoupling. If PostgreSQL goes down, Kafka buffers messages. When it recovers, consumers replay from the last offset. Nothing is lost. Also enables multiple consumers reading the same topic independently.

**Why medallion architecture?**
Bronze preserves raw data forever — if transformation logic has a bug, reprocess from bronze. Silver adds business logic once, cleanly. Gold is optimised for queries. Each layer serves a different consumer with different needs.

**Why dbt and not raw SQL scripts?**
Version control, dependency management, built-in testing, and documentation. dbt runs models in the correct order automatically. Raw SQL scripts require manual execution order management.

**Why Great Expectations alongside dbt tests?**
dbt tests cover structural integrity (not null, uniqueness). GX covers business rules (value ranges, set membership). Together they catch both schema problems and data quality problems.

---

## 📁 Data Flow
Yahoo Finance API
↓ (yfinance Python library)
ftse_producer.py → Kafka: ftse_prices → ftse_consumer.py
↓
raw_ftse_prices (PostgreSQL)
↓ (dbt)
bronze_ftse_prices (view)
↓ (dbt)
silver_ftse_prices (view) ← adds daily_return_pct
↓ (dbt)
fact_daily_prices (table) ← adds sector_rank, volume_rank
mart_sector_performance (table) ← aggregated by sector
↓
Streamlit dashboard · Metabase BI

---

## 🏦 Real World Context

This architecture mirrors what financial institutions run in production:

- **Kafka** — used by every major bank for real-time event streaming
- **dbt** — used by Revolut, Monzo, and most modern data teams
- **Medallion architecture** — standard pattern at Databricks customers
- **Airflow** — industry standard orchestrator at scale
- **Great Expectations** — used for data contracts in regulated industries

---

*Built with Python 3.13 · dbt 1.9.8 · Airflow 3.2.2 · Kafka 7.4.0 · PostgreSQL 15*

