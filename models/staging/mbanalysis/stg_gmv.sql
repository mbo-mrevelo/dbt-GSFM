select
    studioid,
    locationid,
    transdate,
    sum(totalgmv) as totalgmv

from {{ source('mbanalysis', 'gmvdaily') }}

where transdate >= {{ var('start_date') }}
    and transdate < {{ var('end_date') }}

group by
    studioid,
    locationid,
    transdate