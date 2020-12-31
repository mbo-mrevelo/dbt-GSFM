with

apiactivitylog as (

    select * from {{ ref('stg_apiactivitylog') }}

)

select
    apiactivitylog.metricdate,
    apiactivitylog.studioid,
    case when sum(apiactivitylog.brandedweb) > 0 then 1 else 0 end as brandedwebenabled,
    case when sum(apiactivitylog.brandedweb) > 0 then 1 else 0 end as brandedwebutilized,
    sum(apiactivitylog.brandedweb) as brandedwebqualified,
    case when sum(apiactivitylog.apipartners) > 0 then 1 else 0 end as apipartnersenabled,
    case when sum(apiactivitylog.apipartners) > 0 then 1 else 0 end as apipartnersutilized,
    sum(apiactivitylog.apipartners) as apipartnersqualified

from apiactivitylog

group by
    metricdate,
    studioid