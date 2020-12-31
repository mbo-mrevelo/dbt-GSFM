with

tblcctrans as (

    select * from {{ ref('stg_tblcctrans') }}

    where authtime::date >= {{ var('start_date') }}
        and authtime::date < {{ var('end_date') }}
        and settled = 1
        and achlastfour is null

)

select
    date_trunc('day', tblcctrans.authtime) as metricdate,
    tblcctrans.studioid,
    tblcctrans.locationid,
    count(*) as paymentssettledtransactions

from tblcctrans

group by
    metricdate,
    tblcctrans.studioid,
    tblcctrans.locationid

