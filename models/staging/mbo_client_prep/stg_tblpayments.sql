select
    studioid,
    saleid,
    paymentamount,
    paymenttax,
    paymentmethod

from {{ source('mbo_client_prep', 'tblpayments') }}