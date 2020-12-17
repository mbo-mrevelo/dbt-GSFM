select
    studioid,
    createddatetimeutc,
    classdate,
    location,
    sourceid,
    restsource,
    webscheduler,
    visittype,
    typegroup,
    cancelled,
    missed

from {{ source('mbo_client_prep', 'visit_data') }}

