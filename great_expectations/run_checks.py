# great_expectations/run_checks.py
# Connects GX to our PostgreSQL gold layer, defines expectations,
# runs validation, and prints results.

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
import great_expectations.expectations as gxe
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     os.environ.get("POSTGRES_PORT", "5432"),
    "dbname":   os.environ.get("POSTGRES_DB", "lakehouse"),
    "user":     os.environ.get("POSTGRES_USER", "lakehouse_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

CONNECTION_STRING = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

GX_PROJECT_ROOT = os.path.dirname(__file__)

# Load the existing GX project we created in setup_gx.py
context = gx.get_context(mode="file", project_root_dir=GX_PROJECT_ROOT)

# ── Connect to PostgreSQL ─────────────────────────────────────────────────────
# A "data source" in GX terms is a connection to where your data lives.
# This is conceptually similar to a dbt profile — it tells GX how to reach
# our PostgreSQL database.
data_source_name = "lakehouse_postgres"

try:
    data_source = context.data_sources.get(data_source_name)
    print(f"Using existing data source: {data_source_name}")
except (KeyError, LookupError):
    data_source = context.data_sources.add_postgres(
        name=data_source_name,
        connection_string=CONNECTION_STRING,
    )
    print(f"Created new data source: {data_source_name}")


def get_or_create_asset(table_name):
    """
    An 'asset' in GX is a specific table or query within a data source.
    Think of it like a dbt source table — it points GX at exactly
    which table to validate.
    """
    try:
        asset = data_source.get_asset(table_name)
    except LookupError:
        asset = data_source.add_table_asset(name=table_name, table_name=table_name)
    return asset


def get_or_create_batch_definition(asset, name):
    """
    A 'batch definition' tells GX how to slice the data for validation.
    'whole table' means validate everything in the table at once —
    appropriate for our use case since our gold tables are small.
    """
    try:
        return asset.get_batch_definition(name)
    except LookupError:
        return asset.add_batch_definition_whole_table(name)


# ── Define expectations for fact_daily_prices ─────────────────────────────────
print("\n--- Validating fact_daily_prices ---")

asset = get_or_create_asset("fact_daily_prices")
batch_def = get_or_create_batch_definition(asset, "fact_daily_prices_batch")
batch = batch_def.get_batch()

price_expectations = [
    gxe.ExpectColumnValuesToNotBeNull(column="ticker"),
    gxe.ExpectColumnValuesToNotBeNull(column="close_price"),
    gxe.ExpectColumnValuesToBeBetween(column="close_price", min_value=0, max_value=1000000),
    gxe.ExpectColumnValuesToBeBetween(column="volume", min_value=0, max_value=1000000000),
    # A daily move beyond +/- 50% almost certainly indicates a data error
    gxe.ExpectColumnValuesToBeBetween(column="daily_return_pct", min_value=-50, max_value=50),
    gxe.ExpectColumnValuesToBeInSet(
        column="sector",
        value_set=["Financials", "Energy", "Healthcare", "Consumer",
                   "Technology", "Materials", "Utilities"]
    ),
]

for expectation in price_expectations:
    result = batch.validate(expectation)
    status = "PASS" if result.success else "FAIL"
    print(f"  [{status}] {expectation.__class__.__name__} on column '{getattr(expectation, 'column', 'N/A')}'")


# ── Define expectations for fact_fx_rates ──────────────────────────────────────
print("\n--- Validating fact_fx_rates ---")

asset = get_or_create_asset("fact_fx_rates")
batch_def = get_or_create_batch_definition(asset, "fact_fx_rates_batch")
batch = batch_def.get_batch()

fx_expectations = [
    gxe.ExpectColumnValuesToNotBeNull(column="currency_pair"),
    gxe.ExpectColumnValuesToNotBeNull(column="exchange_rate"),
    # Sensible bounds for GBP pairs — catches obviously wrong data
    # e.g. a rate of 0 or 1000 would indicate something went badly wrong upstream
    gxe.ExpectColumnValuesToBeBetween(column="exchange_rate", min_value=0.1, max_value=10),
    gxe.ExpectColumnValuesToBeInSet(
        column="from_currency",
        value_set=["GBP"]
    ),
]

for expectation in fx_expectations:
    result = batch.validate(expectation)
    status = "PASS" if result.success else "FAIL"
    print(f"  [{status}] {expectation.__class__.__name__} on column '{getattr(expectation, 'column', 'N/A')}'")


# ── Define expectations for mart_macro_dashboard ────────────────────────────────
print("\n--- Validating mart_macro_dashboard ---")

asset = get_or_create_asset("mart_macro_dashboard")
batch_def = get_or_create_batch_definition(asset, "mart_macro_dashboard_batch")
batch = batch_def.get_batch()

macro_expectations = [
    gxe.ExpectColumnValuesToNotBeNull(column="indicator"),
    gxe.ExpectColumnValuesToNotBeNull(column="value"),
    # BOE base rate and inflation should be sensible percentages
    # Catches data errors like a misplaced decimal point (e.g. 375 instead of 3.75)
    gxe.ExpectColumnValuesToBeBetween(column="value", min_value=-20, max_value=100),
]

for expectation in macro_expectations:
    result = batch.validate(expectation)
    status = "PASS" if result.success else "FAIL"
    print(f"  [{status}] {expectation.__class__.__name__} on column '{getattr(expectation, 'column', 'N/A')}'")

print("\nValidation complete.")
