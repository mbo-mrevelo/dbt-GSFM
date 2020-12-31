with

visit_data as (

    select * from {{ ref('stg_visit_data') }}

    where (visittype != 1 or visittype is null)
        and createddatetimeutc::date >= {{ var('start_date') }}
        and createddatetimeutc::date < {{ var('end_date') }}

),

tbltypegroup as (

    select * from {{ ref('stg_tbltypegroup') }}

    where wsreservation = 1 or wsenrollment = 1

),

tblreservation as (

    select * from {{ ref('stg_tblreservation') }}

    where visittype != -1
        and createddatetimeutc::date >= {{ var('start_date') }}
        and createddatetimeutc::date < {{ var('end_date') }} 

),

bookings as (

    select
        visit_data.studioid,
        visit_data.createddatetimeutc,
        tbltypegroup.wsreservation,
        tbltypegroup.wsenrollment,
        false as wsappointment,
        visit_data.location,
        visit_data.sourceid,
        visit_data.restsource,
        visit_data.webscheduler
    
    from visit_data

    inner join tbltypegroup
        on tbltypegroup.studioid = visit_data.studioid
            and tbltypegroup.typegroupid = visit_data.typegroup

    union all

    select
        studioid,
        createddatetimeutc,
        false as wsreservation,
        false as wsenrollment,
        true as wsappointment,
        location,
        sourceid,
        restsource,
        webscheduler

    from tblreservation

)

select
    date_trunc('day', createddatetimeutc) as metricdate,
    studioid,
    location as locationid,
    sum(case
            when wsreservation = 1
            then 1
            else 0
        end) as classbookings,
    sum(case
            when wsenrollment = 1
            then 1
            else 0
        end) as eventsbookings,
    sum(case
            when wsappointment = 1
            then 1
            else 0
        end) as appointmentbookings,
    sum(case
            when webscheduler = 1 
                and sourceid is null
                and (restsource is null or lower(restsource) = 'mindbody core software')
            then 1
            else 0
        end) as consumermodebookings,
    sum(case
            when (sourceid = 4788 or restsource ilike any ('%connect%', 'testclient'))
            then 1
            else 0
        end) as mbappbookings,
    sum(case
            when (restsource ilike any ('%engage%', 'fitnessmobileapps') or sourceid in (2785,9340,9341,11178,11179,11841,13803))
            then 1
            else 0
        end) as brandedappbookings,
    sum(case
            when lower(restsource) = 'rwg affiliate production'
            then 1
            else 0
        end) as mbnetworkbookings,
    sum(case
            when webscheduler = 1
            then 1
            else 0
        end) as cntonlinebookings,
    count(*) as cntbookings

from bookings

group by
    metricdate,
    studioid,
    locationid
