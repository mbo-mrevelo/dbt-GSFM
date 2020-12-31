with

tblcctrans as (

    select * from {{ ref('stg_tblcctrans') }}

    where createddatetimeutc::date >= {{ var('start_date') }}
        and createddatetimeutc::date < {{ var('end_date') }}

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