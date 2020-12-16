select
    studioid,
    createddatetimeutc,
    location,
    sourceid,
    restsource,
    webscheduler

from {{ source('mbo_client_prep', 'tblreservation') }}
