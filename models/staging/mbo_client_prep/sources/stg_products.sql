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
    typegroup,
    description,
    createddatetimeutc

from {{ source('mbo_client_prep', 'products') }}
