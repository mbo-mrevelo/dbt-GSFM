select
    studioid,
    saleid,
    sdid,
    productid,
    introoffertype,
    promotedinconnect,
    dynamicservicepricingoption

from {{ source('mbo_client_prep', 'sales_details') }}