select
    studioid,
    enddate,
    unavailable

from {{ source('mbo_client_prep', 'tbltrainerschedules') }}