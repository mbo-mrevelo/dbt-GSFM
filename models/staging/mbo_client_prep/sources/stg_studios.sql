select
    try_to_numeric(studioid) as studioid,
    studioname,
    dbpath,
    studioshort,
    sitecreationdate,
    case 
        when mbgo = 1 then 'Solo'
        when mbbasic = 1 then 'Essential'
        when mbpro = 1 then 'Pro'
        when mbaccelerate = 1 then 'Accelerate'
        when mbconnectlisting = 1 then 'ConnectListing'
        else 'Legacy'
    end as softwarelevel,
    case
        when trim(swtype) = 'CP' then 'childrensProgram' 
        when trim(swtype) = 'FI' then 'healthClub' 
        when trim(swtype) = 'FT' then 'fitnessStudio' 
        when trim(swtype) = 'MA' then 'martialArts' 
        when trim(swtype) = 'O' then  'other' 
        when trim(swtype) = 'OS' then 'yoga' 
        when trim(swtype) = 'PA' then 'partnerStore'
        when trim(swtype) = 'PS' then 'pilates' 
        when trim(swtype) = 'PT' then 'personalTraining' 
        when trim(swtype) = 'R' then  'retail' 
        when trim(swtype) = 'SA' then 'salonFullService' 
        when trim(swtype) = 'SC' then 'skinCare' 
        when trim(swtype) = 'SN' then 'salon' 
        when trim(swtype) = 'SP' then 'dayHealthSpa' 
        when trim(swtype) = 'SS' then 'dance' 
        when trim(swtype) = 'ST' then 'sportsTraining' 
        when trim(swtype) = 'WC' then 'wellnessCenter' 
        when trim(swtype) = 'BE' then 'beauty'
    end as industry,
    mbfoptin,
    try_to_numeric(regionid) as regionid

from {{ source('mbo_client_prep', 'wsmaster_studios') }}

where sitedeactivated = 0
    and demosite = 0
    and deleted = false
