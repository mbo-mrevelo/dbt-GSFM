select 
    studioid,
    showinconsmode,
    typegroupid,
    active

from {{ source('mbo_client_prep', 'tblvisittypes') }}