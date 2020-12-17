with

visit_data as (

    select * from {{ ref('stg_visit_data') }}

    where classdate >= {{ var('start_date') }}
        and classdate < {{ var('end_date') }}
        and cancelled = 0
        and missed = 0
        and visittype != 1

),

tbltypegroup as (

    select * from {{ ref('stg_tbltypegroup') }}

)

select
    classdate as metricdate,
    visit_data.studioid,
    visit_data.location as locationid,
    sum(case
            when wsreservation = 1
            then 1
            else 0
        end) as schedcompletedclasses,
    sum(case
            when wsappointment = 1
            then 1
            else 0
        end) as schedcompletedappointments

from visit_data

inner join tbltypegroup
    on tbltypegroup.studioid = visit_data.studioid
        and tbltypegroup.typegroupid = visit_data.typegroup

group by
    metricdate,
    visit_data.studioid,
    visit_data.location