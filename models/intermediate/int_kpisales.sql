with

sales as (

    select * from {{ ref('stg_sales') }}

    where saledate >= {{ var('start_date') }}
        and saledate < {{ var('end_date') }} 

),

tblpayments as (

    select * from {{ ref('stg_tblpayments') }}

),

payment_types as (

    select * from {{ ref('stg_payment_types') }}

    where casheq = 1

)

select
    sales.saledate as metricdate,
    sales.studioid,
    sales.locationid,
    sum(tblpayments.paymentamount - tblpayments.paymenttax) as revenuesalestotal

from sales

inner join tblpayments
    on tblpayments.studioid = sales.studioid
        and tblpayments.saleid = sales.saleid

inner join payment_types
    on payment_types.studioid = tblpayments.studioid
        and payment_types.payment_method_id = tblpayments.paymentmethod

group by
    metricdate,
    sales.studioid,
    sales.locationid
    