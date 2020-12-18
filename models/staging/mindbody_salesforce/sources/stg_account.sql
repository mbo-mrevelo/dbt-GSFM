select
    try_to_numeric(id18__c) as salesforceid18,
    try_to_numeric(mindbody_id__c) as mindbody_id__c,
    try_to_numeric(mindbody_type__c) as mindbody_type__c,
    isdeleted

from {{ source('salesforce_mindbody_prep', 'account') }}

where mindbody_type__c ilike 'customer'
    and isdeleted = false