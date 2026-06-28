-- Silver layer: cleaned FX rates

with source as (
    select * from {{ ref('bronze_fx_rates') }}
),

cleaned as (
    select
        id,
        upper(trim(from_currency))              as from_currency,
        upper(trim(to_currency))                as to_currency,
        -- currency pair label e.g. GBP/USD
        upper(trim(from_currency))
            || '/' ||
        upper(trim(to_currency))                as currency_pair,
        cast(exchange_rate as numeric(12,6))    as exchange_rate,
        cast(high as numeric(12,6))             as high_rate,
        cast(low as numeric(12,6))              as low_rate,
        cast(ingested_at as timestamptz)        as ingested_at,
        cast(last_refreshed as varchar(50))     as last_refreshed,
        cast(created_at as timestamptz)         as created_at,
        date(cast(ingested_at as timestamptz))  as rate_date
    from source
    where from_currency is not null
      and to_currency is not null
      and exchange_rate is not null
      and exchange_rate > 0
)

select * from cleaned
