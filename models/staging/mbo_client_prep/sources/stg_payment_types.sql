select
    studioid,
    "item#",
    casheq

from {{ source('mbo_client_prep', 'payment_types') }}