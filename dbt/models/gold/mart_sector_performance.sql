-- Gold layer: sector performance mart
-- Aggregates returns by sector for the sector performance dashboard page

with silver as (
    select * from {{ ref('silver_ftse_prices') }}
),

sector_stats as (
    select
        sector,
        trade_date,
        count(ticker)                       as stock_count,
        round(avg(daily_return_pct), 4)     as avg_return_pct,
        round(max(daily_return_pct), 4)     as best_return_pct,
        round(min(daily_return_pct), 4)     as worst_return_pct,
        sum(volume)                         as total_volume,
        -- how many stocks went up vs down
        count(case when daily_return_pct > 0 then 1 end) as stocks_up,
        count(case when daily_return_pct < 0 then 1 end) as stocks_down
    from silver
    group by sector, trade_date
)

select * from sector_stats
