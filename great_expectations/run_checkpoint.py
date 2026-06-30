# great_expectations/run_checkpoint.py
# Runs all expectations and exits with a clear pass/fail signal.
# Designed to be called by Airflow — exits non-zero if any check fails,
# which causes the Airflow task to fail and trigger a retry/alert.

import great_expectations as gx
import great_expectations.expectations as gxe
import os
import sys
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
context = gx.get_context(mode="file", project_root_dir=GX_PROJECT_ROOT)

data_source_name = "lakehouse_postgres"
try:
    data_source = context.data_sources.get(data_source_name)
except (KeyError, LookupError):
    data_source = context.data_sources.add_postgres(
        name=data_source_name,
        connection_string=CONNECTION_STRING,
    )


def get_or_create_asset(table_name):
    try:
        return data_source.get_asset(table_name)
    except LookupError:
        return data_source.add_table_asset(name=table_name, table_name=table_name)


def get_or_create_batch_definition(asset, name):
    try:
        return asset.get_batch_definition(name)
    except LookupError:
        return asset.add_batch_definition_whole_table(name)


# Each tuple is: (table_name, list_of_expectations)
CHECKS = [
    ("fact_daily_prices", [
        gxe.ExpectColumnValuesToNotBeNull(column="ticker"),
        gxe.ExpectColumnValuesToNotBeNull(column="close_price"),
        gxe.ExpectColumnValuesToBeBetween(column="close_price", min_value=0, max_value=1000000),
        gxe.ExpectColumnValuesToBeBetween(column="volume", min_value=0, max_value=1000000000),
gxe.ExpectColumnValuesToBeBetween(column="daily_return_pct", min_value=-50, max_value=50),        gxe.ExpectColumnValuesToBeInSet(
            column="sector",
            value_set=["Financials", "Energy", "Healthcare", "Consumer",
                       "Technology", "Materials", "Utilities"]
        ),
    ]),
    ("fact_fx_rates", [
        gxe.ExpectColumnValuesToNotBeNull(column="currency_pair"),
        gxe.ExpectColumnValuesToNotBeNull(column="exchange_rate"),
        gxe.ExpectColumnValuesToBeBetween(column="exchange_rate", min_value=0.1, max_value=10),
        gxe.ExpectColumnValuesToBeInSet(column="from_currency", value_set=["GBP"]),
    ]),
    ("mart_macro_dashboard", [
        gxe.ExpectColumnValuesToNotBeNull(column="indicator"),
        gxe.ExpectColumnValuesToNotBeNull(column="value"),
        gxe.ExpectColumnValuesToBeBetween(column="value", min_value=-20, max_value=100),
    ]),
]


def run_all_checks():
    total_checks = 0
    failed_checks = 0
    results_summary = []

    for table_name, expectations in CHECKS:
        asset = get_or_create_asset(table_name)
        batch_def = get_or_create_batch_definition(asset, f"{table_name}_batch")
        batch = batch_def.get_batch()

        for expectation in expectations:
            total_checks += 1
            result = batch.validate(expectation)
            status = "PASS" if result.success else "FAIL"
            if not result.success:
                failed_checks += 1
            results_summary.append(
                f"[{status}] {table_name}.{getattr(expectation, 'column', 'N/A')} "
                f"({expectation.__class__.__name__})"
            )

    print("\n" + "=" * 70)
    print("GREAT EXPECTATIONS CHECKPOINT SUMMARY")
    print("=" * 70)
    for line in results_summary:
        print(line)
    print("=" * 70)
    print(f"Total checks: {total_checks} | Passed: {total_checks - failed_checks} | Failed: {failed_checks}")
    print("=" * 70)

    if failed_checks > 0:
        print(f"\nCHECKPOINT FAILED: {failed_checks} check(s) did not pass")
        sys.exit(1)
    else:
        print("\nCHECKPOINT PASSED: all data quality checks succeeded")
        sys.exit(0)


if __name__ == "__main__":
    run_all_checks()
