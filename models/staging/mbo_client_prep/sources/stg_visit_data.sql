select
    studioid,
    createddatetimeutc,
    location,
    sourceid,
    restsource,
    webscheduler,
    visittype

from {{ source('mbo_client_prep', 'visit_data') }}

