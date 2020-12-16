select
    studioid,
    classpayment,
    classdescriptionid,
    active

from {{ source('mbo_client_prep', 'tblclassdescriptions') }}

where active = 1

order by
    studioid,
    classpayment,
    classdescriptionid
