with

date_spine as (

    select * from {{ ref('stg_filtered_date_spine') }}

),

tblvisittypes as (

    select * from {{ ref('stg_tblvisittypes') }}

    where active = 1

),

tbltypegroup as (

    select * from {{ ref('stg_tbltypegroup') }}

    where active = 1
        and wsappointment = 1

),

tbltrainerschedules as (

    select * from {{ ref('stg_tbltrainerschedules') }}

    where unavailable = 0

),

trainers_and_dates as (

    select
        date_spine.metricdate,
        tbltrainerschedules.studioid

    from tbltrainerschedules

    inner join date_spine
        on date_spine.metricdate <= tbltrainerschedules.enddate::date

)

select
    trainers_and_dates.metricdate,
    tblvisittypes.studioid,
    1 as appointmentsenabled,
    max(case
            when tbltypegroup.wsdisable = 0
                and tblvisittypes.showinconsmode = 1
            then 1
            else 0
        end) as appointmentonlineenabled

from tblvisittypes

inner join tbltypegroup
    on tbltypegroup.studioid = tblvisittypes.studioid
        and tbltypegroup.typegroupid = tblvisittypes.typegroup

inner join trainers_and_dates
    on trainers_and_dates.studioid = tblvisittypes.studioid

group by
    trainers_and_dates.metricdate,
    tblvisittypes.studioid