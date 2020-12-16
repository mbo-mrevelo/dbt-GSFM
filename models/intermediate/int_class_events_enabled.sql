with

tblclassdescriptions as (

    select * from {{ ref('tblclassdescriptions') }}

),

tbltypegroup as (

    select * from {{ ref('tbltypegroup') }}

),

tblclasses as (

    select * from {{ ref('tblclasses') }}

),

tblclasssch as (

    select * from {{ ref('tblclasssch') }}

)

select
    date_trunc('day', tblclasssch.classdate) as metricdate,
    tblclassdescriptions.studioid,
    tblclasses.locationid,
    max(case
            when tbltypegroup.wsreservation = 1 
            then 1
            else 0
        end) as classesenabled,
    max(case
            when tbltypegroup.wsenrollment = 1 
            then 1
            else 0
        end) as eventsenabled,
    sum(tblclasssch.totnumvd) as totnumvd,
    sum(tblclasses.maxcapacity) as maxcapacity,
    max(case
            when tbltypegroup.wsdisable = 0
                and tblclasses.classactive = 1
            then 1
            else 0
        end) as classonlineenabled

from tblclassdescriptions

inner join tbltypegroup
    on tbltypegroup.studioid = tblclassdescriptions.studioid
        and tbltypegroup.typegroupid = tblclassdescriptions.classpayment

inner join tblclasses
    on tblclasses.studioid = tblclassdescriptions.studioid
        and tblclasses.descriptionid = tblclassdescriptions.classdescriptionid

inner join tblclasssch
    on tblclasssch.studioid = tblclasses.studioid
        and tblclasssch.classid = tblclasses.classid

group by
    metrcidate,
    tblclassdescriptions.studioid,
    tblclasses.locationid