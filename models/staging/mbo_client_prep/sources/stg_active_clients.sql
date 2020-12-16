with

date_spine as (

    select * from {{ ref('stg_filtered_date_spine') }}

),

clients as (

    select
        createddatetimeutc,
        studioid,
        count(*) as cnttotalclients

    from {{ source('mbo_client_prep', 'clients') }}

    where clientid > 1
        and issystem = 0
        and deleted = 0
        and mergeclientid is null
        and inactive = 0
        and isprospect != 1

    group by
        createddatetimeutc,
        studioid

)

select
    date_spine.metricdate,
    clients.studioid,
    clients.cnttotalclients

from clients

inner join date_spine on date_spine.metricdate >= clients.createddatetimeutc


 


