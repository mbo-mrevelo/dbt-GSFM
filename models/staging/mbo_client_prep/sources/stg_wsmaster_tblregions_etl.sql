select
    regionsid,
    cr_notification_opt_out

from {{ source('mbo_client_prep', 'wsmaster_tblregions_etl') }}