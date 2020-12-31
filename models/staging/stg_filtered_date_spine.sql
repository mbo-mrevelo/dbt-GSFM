select
    date_day as metricdate

from {{ ref('date_spine_day') }}

where metricdate >= {{ var('start_date') }}
    and metricdate < {{ var('end_date') }}