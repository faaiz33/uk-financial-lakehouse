-- Gold layer: macro dashboard mart
-- Pivots macro indicators into a wide format for easy dashboard consumption

with silver as (
    select * from {{ ref('silver_macro_indicators') }}
),

final as (
    select
        indicator,
        series_code,
        source_system,
        value,
        period,
        ingested_at,
        -- label for display in dashboard
        case indicator
            when 'base_rate'         then 'BOE Base Rate (%)'
            when 'unemployment_rate' then 'Unemployment Rate (%)'
            when 'gdp_growth'        then 'GDP Growth (%)'
            when 'inflation_cpi'     then 'CPI Inflation (%)'
            else indicator
        end                         as display_name
    from silver
)

select * from final
