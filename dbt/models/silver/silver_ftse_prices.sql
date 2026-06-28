-- Silver layer: cleaned and deduplicated FTSE prices
-- Casts types explicitly, handles nulls, adds derived columns

with source as (
    select * from {{ ref('bronze_ftse_prices') }}
),

cleaned as (
    select
        id,
        upper(trim(ticker))                    as ticker,
        initcap(trim(sector))                  as sector,
        cast(open as numeric(12,4))            as open_price,
        cast(high as numeric(12,4))            as high_price,
        cast(low as numeric(12,4))             as low_price,
        cast(close as numeric(12,4))           as close_price,
        cast(volume as bigint)                 as volume,
        -- daily return % = (close - open) / open * 100
        round(
            ((cast(close as numeric) - cast(open as numeric))
            / nullif(cast(open as numeric), 0)) * 100
        , 4)                                   as daily_return_pct,
        cast(ingested_at as timestamptz)       as ingested_at,
        cast(market_timestamp as varchar(50))  as market_timestamp,
        cast(created_at as timestamptz)        as created_at,
        -- partition key for gold layer joins
        date(cast(ingested_at as timestamptz)) as trade_date
    from source
    where ticker is not null
      and close is not null
      and close > 0
)

select * from cleaned
