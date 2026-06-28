-- Bronze layer: raw macro indicators exactly as loaded by the consumer

select
    id,
    indicator,
    series_code,
    source,
    value,
    period,
    ingested_at,
    created_at
from {{ source('public', 'raw_macro_indicators') }}
