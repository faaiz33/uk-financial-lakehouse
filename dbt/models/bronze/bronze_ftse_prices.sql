-- Bronze layer: raw FTSE prices exactly as loaded by the consumer
-- No transformations — this is a clean view over the raw table
-- Materialized as view so it always reflects the latest raw data

select
    id,
    ticker,
    sector,
    open,
    high,
    low,
    close,
    volume,
    ingested_at,
    market_timestamp,
    created_at
from {{ source('public', 'raw_ftse_prices') }}
