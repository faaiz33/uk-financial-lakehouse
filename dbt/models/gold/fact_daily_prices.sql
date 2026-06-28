-- Gold layer: fact table for daily FTSE prices
-- One row per ticker per trade date
-- This is the primary table for the market overview dashboard

with silver as (
    select * from {{ ref('silver_ftse_prices') }}
),

final as (
    select
        ticker,
        sector,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        daily_return_pct,
        -- rank stocks by return within each sector on each day
        rank() over (
            partition by sector, trade_date
            order by daily_return_pct desc
        )                                           as sector_rank,
        -- rank stocks by volume on each day
        rank() over (
            partition by trade_date
            order by volume desc
        )                                           as volume_rank,
        ingested_at,
        market_timestamp
    from silver
)

select * from final
