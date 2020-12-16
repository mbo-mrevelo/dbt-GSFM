select
    studioid,
    useragreementstateid,
    date_trunc('day', dateadded) as dateadded

from {{ source('mbo_client_prep', 'agreements_useragreementsessionstate') }}

where agreementversionid in (7, 8)

qualify row_number() over (partition by studioid, date_trunc('day', dateadded) order by useragreementsessionstateid desc) = 1