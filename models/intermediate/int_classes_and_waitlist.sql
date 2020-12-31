with

tblclasses as (

    select * from {{ ref('stg_tblclasses') }}

),

tblwaitlist as (

    select * from {{ ref('stg_tblwaitlist') }}

    where createddatetimeutc::date >= {{ var('start_date') }}
        and createddatetimeutc::date < {{ var('end_date') }}

)

select
    date_trunc('day', tblwaitlist.createddatetimeutc) as metricdate,
    tblwaitlist.studioid,
    tblclasses.locationid,
    count(distinct tblwaitlist.classid) as cntdistinctclasses,
    count(distinct tblwaitlist.clientid) as cntdistinctclients

from tblwaitlist

inner join tblclasses
    on tblclasses.studioid = tblwaitlist.studioid
        and tblclasses.classid = tblwaitlist.classid

group by
    metricdate,
    tblwaitlist.studioid,
    tblclasses.locationid