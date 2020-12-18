select
    id18__c as salesforceid18,
    mindbody_id__c,
    mindbody_type__c,
    isdeleted

from {{ source('salesforce_mindbody_prep', 'account') }}

where mindbody_type__c ilike 'customer'
    and isdeleted = false