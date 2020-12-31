with

sales as (

    select * from {{ ref('stg_sales') }}

),

sales_details as (

    select * from {{ ref('stg_sales_details') }}

),

products as (

    select * from {{ ref('stg_products') }}

)

select
    date_trunc('day', sales.createddatetimeutc) as metricdate,
    sales.studioid,
    sales.locationid,
    coalesce(count(distinct case
                                when products.itemtypeid = 4 
                                then sales_details.sdid
                                else null
                            end
                    ), 0) as cntgiftcardssold,
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and products.count >= 1000
                                then sales_details.sdid
                                else null
                            end
                    ), 0) as cntunlimitedssold,
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and products.count = 1
                                then sales_details.sdid
                                else null
                            end
                    ), 0) as cntdropinssold,
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and products.count > 1
                                    and products.count < 1000
                                then sales_details.sdid
                                else null
                            end
                    ), 0) as cntmultissold,
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and (sales_details.introoffertype > 1 or products.introductory = 1)
                                then sales_details.sdid
                                else null
                            end
                    ), 0) as cntintroofferssold,
    count(distinct sales.saleid) as cnttotalsales

from sales

inner join sales_details
    on sales_details.studioid = sales.studioid
        and sales_details.saleid = sales.saleid

inner join products
    on products.studioid = sales_details.studioid
        and products.productid = sales_details.productid

where sales.createddatetimeutc::date >= {{ var('start_date') }}
    and sales.createddatetimeutc::date < {{ var('end_date') }}

group by
    metricdate,
    sales.studioid,
    sales.locationid

    