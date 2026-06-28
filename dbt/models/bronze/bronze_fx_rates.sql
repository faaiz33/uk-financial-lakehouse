-- Bronze layer: raw FX rates exactly as loaded by the consumer

select
    id,
    from_currency,
    to_currency,
    exchange_rate,
    high,
    low,
    volume,
    last_refreshed,
    ingested_at,
    created_at
from {{ source('public', 'raw_fx_rates') }}
