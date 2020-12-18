select
    studioid,
    saleid,
    sdid,
    productid,
    introoffertype

from {{ source('mbo_client_prep', 'sales_details') }}