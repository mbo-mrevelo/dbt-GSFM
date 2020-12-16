select
    studioid,
    productid,
    itemtypeid,
    count,
    introductory

from {{ source('mbo_client_prep', 'products') }}
