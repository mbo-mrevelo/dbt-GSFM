select
    createddatetimeutc,
    saledate,
    studioid,
    locationid,
    saleid,
    restsource,
    sourceid

from {{ source('mbo_client_prep', 'sales') }}