select
    studioid,
    productid,
    itemtypeid,
    count,
    introductory,
    wsshow,
    sellinmarketplace

from {{ source('mbo_client_prep', 'products') }}
