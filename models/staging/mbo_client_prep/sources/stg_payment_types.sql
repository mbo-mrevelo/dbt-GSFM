select
    studioid,
    "Item#" as payment_method_id,
    casheq

from {{ source('mbo_client_prep', 'payment_types') }}