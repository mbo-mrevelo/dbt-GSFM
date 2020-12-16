select
    studioid,
    productid,
    itemtypeid,
    count,
    introductory,
    wsshow,
    sellinmarketplace,
    discontinued,
    deleted,
    type,
    description

from {{ source('mbo_client_prep', 'products') }}
