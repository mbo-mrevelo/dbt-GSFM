with

date_spine as (

    select * from {{ ref('stg_filtered_date_spine') }}

),

products as (

    select * from {{ ref('stg_products') }}

    where discontinued = 0
        and deleted = 0
        and type != 9
        and itemtypeid != 0
        and lower(description) != 'dynamic pricing'

),

tbltypegroup as (

    select * from {{ ref('stg_tbltypegroup') }}

    where active = 1

)

select
    date_spine.metricdate,
    products.studioid,
    1 as haspos,
    max(case
            when wsshow = 1
            then 1
            else 0
        end) as onlinepos,
    max(case
            when itemtypeid = 1
                and sellinmarketplace = 1
                and introductory = 1
                and wsshow = 1
            then 1
            else 0
        end) as cntintrooffersmarkedpromotedinapp

from products

inner join tbltypegroup
    on tbltypegroup.studioid = products.studioid
        and tbltypegroup.typegroupid = products.typegroupid

inner join date_spine
    on date_spine.metricdate >= procucts.createddatetimeutc

group by
    metricdate,
    products.studioid