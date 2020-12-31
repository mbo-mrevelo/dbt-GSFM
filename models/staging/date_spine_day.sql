with

date_spine as (

    {{ dbt_utils.date_spine("day", "'2015-01-01'", "'2030-01-01'") }}

),

final as (

    select
        cast(date_day as date) as date_day,
        cast(date_trunc('week', date_day) as date) as start_of_week,
        cast(last_day(date_day, 'week') as date) as end_of_week

    from date_spine

)

select * from final