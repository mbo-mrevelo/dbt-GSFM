with

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
        and createddatetimeutc::date <= {{ var('end_date') }}

    group by
        createddatetimeutc,
        studioid

)

select
    clients.createddatetimeutc::date as metricdate,
    clients.studioid,
    sum(clients.cnttotalclients) over (partition by studioid order by metricdate) as cnttotalclients

from clients

qualify last_value(clients.createddatetimeutc) over (partition by clients.studioid order by clients.createddatetimeutc) = clients.createddatetimeutc


 


