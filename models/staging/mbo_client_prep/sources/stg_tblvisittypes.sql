select 
    studioid,
    showinconsmode,
    typegroup,
    active

from {{ source('mbo_client_prep', 'tblvisittypes') }}