select
    studioid,
    typegroupid,
    wsreservation,
    wsenrollment,
    wsdisable

from {{ source('mbo_client_prep', 'tbltypegroup') }}

where wsreservation = 1 or
    wsenrollment = 1

order by
    studioid,
    typegroupid