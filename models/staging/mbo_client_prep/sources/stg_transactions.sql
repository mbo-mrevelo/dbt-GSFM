select
    date_trunc('day', createddatetimeutc) as metricdate,
    studioid,
    locationid,
    count(distinct transactionnumber) as cnttransactions,
    count(distinct
            case
                when ccswiped = 1 then transactionnumber
                else null
            end 
          ) as cnttransactionsswiped
        
    
from {{ source('mbo_client_prep', 'tblcctrans') }}

where createddatetimeutc >= {{ var('start_date') }}
    and createddatetimeutc < {{ var('end_date') }}
    
group by
    metricdate,
    studioid,
    locationid