select
    studioid,
    classpayment,
    classdescriptionid,
    active

from {{ source('mbo_client_prep', 'tblclassdescriptions') }}

order by
    studioid,
    classpayment,
    classdescriptionid
