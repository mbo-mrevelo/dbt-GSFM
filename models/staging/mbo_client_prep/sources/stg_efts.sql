select
    date_trunc('day', rundatetime) as metricdate,
    studioid,
    saleloc as locationid,
    count(*) as cntautopays

from {{ source('mbo_client_prep', 'tbleftschedule') }}

where saleid is not null
    and deleted = 0

group by
    metricdate,
    studioid,
    locationid