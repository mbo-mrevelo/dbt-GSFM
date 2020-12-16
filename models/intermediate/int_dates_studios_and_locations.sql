with

date_spine as (

    select * from {{ ref('stg_filtered_date_spine') }}

),

studios as (

    select * from {{ ref('stg_studios') }}

),

locations as (

    select * from {{ ref('stg_locations') }}

)

select
    date_spine.metricdate,
    studios.studioid,
    studios.studioname,
    studios.regionid,
    studios.dbpath,
    studios.studioshort,
    studios.sitecreationdate,
    studios.softwarelevel,
    studios.industry,
    studios.mbfoptin,
    locations.locationid,
    locations.mbaccountnumber,
    locations.masterlocid,
    locations.hasmap

from studios

inner join locations
    on locations.studioid = studios.studioid

cross join date_spine