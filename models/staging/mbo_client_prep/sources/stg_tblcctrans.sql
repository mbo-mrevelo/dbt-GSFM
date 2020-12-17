select
    createddatetimeutc,
    studioid,
    locationid,
    transactionnumber,
    ccswiped,
    authtime,
    settled,
    achlastfour

from {{ source('mbo_client_prep', 'tblcctrans') }}