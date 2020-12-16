select
    studioid,
    classid,
    classdate,
    totnumvd

from {{ source('mbo_client_prep', 'tblclasssch') }}

order by
    studioid,
    typegroupid