select
    date_trunc('day', createddatetimeutc) as metricdate,
    studioid,
    classid,
    clientid

from {{ source('mbo_client_prep', 'tblwaitlist') }}

where createddatetimeutc >= {{ var('start_date') }}
    and createddatetimeutc < {{ var('end_date') }}