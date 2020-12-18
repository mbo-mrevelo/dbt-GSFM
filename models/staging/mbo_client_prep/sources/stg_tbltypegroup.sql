select
    studioid,
    typegroupid,
    wsreservation,
    wsenrollment,
    wsdisable,
    wsappointment,
    active

from {{ source('mbo_client_prep', 'tbltypegroup') }}

order by
    studioid,
    typegroupid