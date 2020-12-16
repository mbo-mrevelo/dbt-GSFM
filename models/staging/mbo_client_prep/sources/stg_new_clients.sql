select
    date_trunc('day', createddatetimeutc) as metricdate,
    studioid,
    count(*) as cntnewclients,
    sum(case
            when sourceid = 4788 
                or lower(restsource) in ('ios connect', 'android connect', 'testclient', 'connect backfill')
                then 1
                else 0
        end) as cntmbappdiscoveries

from {{ source('mbo_client_prep', 'clients') }}

where createddatetimeutc >= {{ var('start_date') }}
    and createddatetimeutc < {{ var('end_date') }}
    and clientid > 1
    and issystem = 0
    and deleted = 0
    and mergeclientid is null

group by
    metricdate,
    studioid