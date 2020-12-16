select
    studioid,
    locationid,
    classid,
    descriptionid,
    maxcapacity,
    classactive

from {{ source('mbo_client_prep', 'tblclasses') }}

where classdatestart >= {{ var('start_date') }}
    and classdateend < {{ var('end_date') }}

order by
    studioid,
    classid,
    descriptionid