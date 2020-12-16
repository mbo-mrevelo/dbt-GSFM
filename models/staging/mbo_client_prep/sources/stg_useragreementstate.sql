select
    useragreementstateid,
    useragreementstatename

from {{ source('mbo_client_prep', 'agreements_useragreementstate') }}