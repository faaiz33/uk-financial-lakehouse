# great_expectations/setup_gx.py
# Sets up Great Expectations context, connects to our gold layer,
# and defines expectations (data quality rules) for our tables.

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
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

# get_context() creates (or loads) a Great Expectations project
# This creates a 'gx' folder with all GX configuration, similar to how
# dbt_project.yml configures a dbt project
context = gx.get_context(mode="file", project_root_dir=os.path.dirname(__file__))

print("Great Expectations context created successfully")
print(f"Project root: {context.root_directory}")
