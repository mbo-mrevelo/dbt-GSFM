with

tblclasses as (

    select * from {{ ref('stg_tblclasses') }}

),

tblwaitlist as (

    select * from {{ ref('stg_tblwaitlist') }}

)

select
    tblwaitlist.metricdate,
    tblwaitlist.studioid,
    tblclasses.locationid,
    count(distinct tblwaitlist.classid) as cntdistinctclasses,
    count(distinct tblwaitlist.clientid) as cntdistinctclients

from tblwaitlist

inner join tblclasses
    on tblclasses.studioid = tblwaitlist.studioid
        and tblclasses.classid = tblwaitlist.classid

group by
    tblwaitlist.metricdate,
    tblwaitlist.studioid,
    tblclasses.locationid