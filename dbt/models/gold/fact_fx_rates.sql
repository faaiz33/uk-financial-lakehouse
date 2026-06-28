-- Gold layer: fact table for FX rates

with silver as (
    select * from {{ ref('silver_fx_rates') }}
),

final as (
    select
        currency_pair,
        from_currency,
        to_currency,
        exchange_rate,
        high_rate,
        low_rate,
        -- spread = difference between high and low
        round(high_rate - low_rate, 6)  as rate_spread,
        rate_date,
        last_refreshed,
        ingested_at
    from silver
)

select * from final
