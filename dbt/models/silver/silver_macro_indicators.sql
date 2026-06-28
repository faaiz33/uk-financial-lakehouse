-- Silver layer: cleaned macro indicators

with source as (
    select * from {{ ref('bronze_macro_indicators') }}
),

cleaned as (
    select
        id,
        lower(trim(indicator))                  as indicator,
        upper(trim(series_code))                as series_code,
        trim(source)                            as source,
        cast(value as numeric(12,4))            as value,
        trim(period)                            as period,
        cast(ingested_at as timestamptz)        as ingested_at,
        cast(created_at as timestamptz)         as created_at,
        -- flag whether this is a BOE or ONS sourced indicator
        case
            when lower(trim(source)) like '%bank of england%' then 'BOE'
            when lower(trim(source)) like '%ons%' then 'ONS'
            else 'UNKNOWN'
        end                                     as source_system
    from source
    where indicator is not null
      and value is not null
)

select * from cleaned
