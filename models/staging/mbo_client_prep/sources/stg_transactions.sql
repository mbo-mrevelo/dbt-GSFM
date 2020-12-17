with

tblcctrans as (

    select * from {{ ref('stg_tblcctrans') }}

    where createddatetimeutc >= {{ var('start_date') }}
        and createddatetimeutc < {{ var('end_date') }}

)

select
    date_trunc('day', tblcctrans.createddatetimeutc) as metricdate,
    tblcctrans.studioid,
    tblcctrans.locationid,
    count(distinct tblcctrans.transactionnumber) as cnttransactions,
    count(distinct
            case
                when tblcctrans.ccswiped = 1 then tblcctrans.transactionnumber
                else null
            end 
          ) as cnttransactionsswiped
        
    
from tblcctrans
    
group by
    metricdate,
    tblcctrans.studioid,
    tblcctrans.locationid