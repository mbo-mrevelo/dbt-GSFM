select
    createddatetimeutc,
    studioid,
    classid,
    clientid

from {{ source('mbo_client_prep', 'tblwaitlist') }}