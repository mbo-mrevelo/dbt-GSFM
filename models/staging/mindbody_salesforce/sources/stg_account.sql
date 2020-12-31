select
    id18__c as salesforceid18,
    try_to_numeric(mindbody_id__c) as mindbody_id__c,
    mindbody_type__c,
    isdeleted

from {{ source('salesforce_mindbody_prep', 'account') }}

where lower(mindbody_type__c) = 'customer'
    and isdeleted = false
