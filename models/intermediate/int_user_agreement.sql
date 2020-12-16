with

date_spine as (

    select * from {{ ref('stg_filtered_date_spine') }}

),

recentagreement as (

    select * from {{ ref('stg_useragreementsessionstate') }}

),

useragreementstate as (

select * from {{ ref('stg_useragreementstate') }}

)

select
    date_spine.metricdate,
    recentagreement.studioid,
    useragreementstate.useragreementstatename as currentoptinstatus

from recentagreement 

inner join date_spine on date_spine.metricdate >= recentagreement.dateadded

inner join useragreementstate on useragreementstate.useragreementstateid = recentagreement.useragreementstateid

qualify row_number() over(partition by date_spine.metricdate, recentagreement.studioid order by recentagreement.dateadded desc) = 1