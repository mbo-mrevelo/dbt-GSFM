 select
    date_trunc('day', createddatetimeutc) as metricdate,
    studioid,
    sum(case
            when lower(contactmethod) = 'sms' 
                and autoemailid in (9,10,11,12,25,26,31,32,44,52,54,55)
            then 1
            else 0
        end
        ) as cntapptsmssent,
    sum(case
            when lower(contactmethod) = 'sms' 
                and autoemailid in (17,56)
            then 1
            else 0
        end
        ) as cntwaitlistsmssent,
    sum(case
            when lower(contactmethod) = 'e-mail' 
                and autoemailid in (59,63,64)
            then 1
            else 0
        end
        ) as cntcheckinginemailsent,
    sum(case
            when lower(contactmethod) = 'e-mail' 
                and autoemailid in (60,61,62)
            then 1
            else 0
        end
        ) as cntwemissyouemailsent,
    count(1) as acceleratequalified
    
from {{ source('mbo_client_prep', 'tblcontactlogs') }}

where contactmethod ilike ('e-mail', 'sms')
    and autoemailid in (9,10,11,12,25,26,31,32,44,52,54,55,17,56,59,60,61,62,63,64)
    and createddatetimeutc >= {{ var('start_date') }}
    and createddatetimeutc < {{ var('end_date') }}
    
group by
    metricdate,
    studioid