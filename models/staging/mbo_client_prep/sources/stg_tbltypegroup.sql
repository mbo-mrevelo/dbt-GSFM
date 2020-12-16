select
    studioid,
    typegroupid,
    wsreservation,
    wsenrollment,
    wsdisable,
    wsappointment

from {{ source('mbo_client_prep', 'tbltypegroup') }}

order by
    studioid,
    typegroupid