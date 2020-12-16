select
    createddatetimeutc,
    studioid,
    locationid,
    saleid,
    restsource,
    sourceid

from {{ source('mbo_client_prep', 'sales') }}