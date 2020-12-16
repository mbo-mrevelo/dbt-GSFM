select
    studioid,
    typegroupid,
    wsreservation,
    wsenrollment,
    wsdisable

from {{ source('mbo_client_prep', 'tbltypegroup') }}

order by
    studioid,
    typegroupid