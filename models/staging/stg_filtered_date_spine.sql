select
    date_day as metricdate

from {{ ref('base_date_spine_day') }}

where metricdate >= {{ var('start_date') }}
    and metricdate < {{ var('end_date') }}