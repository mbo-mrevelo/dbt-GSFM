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
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and (sales_details.introoffertype > 1 or products.introductory = 1)
                                    and (sales.restsource ilike any ('%engage%', 'fitnessmobileapps')
                                        or sales.sourceid in (2785,9340,9341,11178,11179,11841,13803))
                                then sales_details.sdid
                                else null
                            end
                    ) , 0) as cntbrandedappintroofferssold,
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and sales_details.introoffertype > 1
                                    and (sales.restsource ilike any ('%connect%', 'testclient')
                                        or sales.sourceid = 4788)
                                then sales_details.sdid
                                else null
                            end
                    ) , 0) as cntmbappintroofferssold,
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and sales_details.introoffertype > 1
                                    and sales_details.promotedinconnect = 1
                                    and (sales.restsource ilike any ('%connect%', 'testclient', 'lymber api gateway client')
                                        or sales.sourceid = 4788)
                                then sales_details.sdid
                                else null
                            end
                    ) , 0) as cntpromotedintroofferssold,
    coalesce(count(distinct case
                                when sales_details.dynamicservicepricingoption = 1
                                then sales_details.sdid
                                else null
                            end
                    ) , 0) as cntdynamicpricingsold,
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and (sales_details.introoffertype > 1 or products.introductory = 1)
                                    and sales.sourceid is null
                                    and (sales.restsource is null or lower(sales.restsource) = 'mindbody core software')
                                    and sales.locationid = 98
                                then sales_details.sdid
                                else null
                            end
                    ) , 0) as cntconsumermodeintroofferssold,
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and (sales_details.introoffertype > 1 or products.introductory = 1)
                                    and sales.sourceid in (3342,313,284,18281)
                                then sales_details.sdid
                                else null
                            end
                    ) , 0) as cntbrandedwebintroofferssold,
    coalesce(count(distinct case
                                when products.itemtypeid = 1
                                    and (sales_details.introoffertype > 1 or products.introductory = 1)
                                    and (sales.locationid = 98 or
                                            (sales.restsource is not null and not(sales.restsource ilike any ('%kiosk%', '%express%', '%hathway%', 'mattholdenapicreds', 'mindbody core software'))) or
                                            (sales.sourceid is not null and sales.sourceid not in (5325,4031,5324,5150,5819,6637,6660,8371,9342,9343,11448)))
                                then sales_details.sdid
                                else null
                            end
                    ) , 0) as cntintroofferssoldonline,
    coalesce(count(distinct case
                                when (sales.locationid = 98 or
                                        (sales.restsource is not null and not(sales.restsource ilike any ('%kiosk%', '%express%', '%hathway%', 'mattholdenapicreds', 'mindbody core software'))) or
                                        (sales.sourceid is not null and sales.sourceid not in (5325,4031,5324,5150,5819,6637,6660,8371,9342,9343,11448)))
                                then sales.saleid
                                else null
                            end
                    ) , 0) as cntonlinesales,
    count(distinct
                case
                    when sales.restsource ilike any ('%engage%', 'fitnessmobileapps') or
                        sales.sourceid in (2785,9340,9341,11178,11179,11841,13803)
                    then sales.saleid
                    else null
                end
        ) as cntbrandedappsales

from sales

inner join sales_details on sales_details.studioid = sales.studioid and sales_details.saleid = sales.saleid

inner join products on products.studioid = sales_details.studioid and products.productid = sales_details.productid

where sales.createddatetimeutc::date >= {{ var('start_date') }}
    and sales.createddatetimeutc::date < {{ var('end_date') }}

group by
    metricdate,
    sales.studioid
