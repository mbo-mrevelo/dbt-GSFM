select
    studioid,
    classid,
    classdate,
    totnumvd

from {{ source('mbo_client_prep', 'tblclasssch') }}

where classdate >= {{ var('start_date') }}
    and classdate < {{ var('end_date') }}

order by
    studioid,
    typegroupid