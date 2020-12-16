select
    studioid,
    enablemarketplacedeals

from {{ source('mbo_client_prep', 'tblgenopts') }}