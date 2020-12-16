select
    studioid,
    locationid,
    classid,
    descriptionid,
    maxcapacity,
    classactive,
    classdatestart,
    classdateend

from {{ source('mbo_client_prep', 'tblclasses') }}

order by
    studioid,
    classid,
    descriptionid