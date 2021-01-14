{{ config(materialized = 'table') }}

with

dates_studios_and_locations as (

    select * from {{ ref('int_dates_studios_and_locations') }}

),

salesforce_account as (

    select * from {{ ref('stg_account') }}

),

class_events_enabled as (

    select * from {{ ref('int_class_events_enabled') }}

),

bookings_data as (

    select * from {{ ref('int_bookings_data') }}

),

appointments_enabled as (

    select * from {{ ref('int_appointments_enabled') }}

),

pos as (

    select * from {{ ref('int_pos') }}

),

sales as (

    select * from {{ ref('int_sales') }}

),

transactions as (

    select * from {{ ref('stg_transactions') }}

),

sms_email as (

    select * from {{ ref('stg_smsemail') }}

),

online_sales as (

    select * from {{ ref('int_online_sales') }}

),

efts as (

    select * from {{ ref('stg_efts') }}

),

new_clients as (

    select * from {{ ref('stg_new_clients') }}

),

tblgenopts as (

    select * from {{ref('stg_tblgenopts') }}

),

user_agreement as (

    select * from {{ ref('int_user_agreement') }}

),

classes_and_waitlist as (

    select * from {{ ref('int_classes_and_waitlist') }}

),

active_clients as (

    select * from {{ ref('stg_active_clients') }}

),

kpi_sales as (

    select * from {{ ref('int_kpisales') }}

),

kpi_transactions as (

    select * from {{ ref('stg_kpitransactions') }}

),

schedule_complete as (

    select * from {{ ref('int_schedule_complete') }}

),

gmv as (

    select * from {{ ref('stg_gmv')}}

)

select
-- dates_studios_and_locations
    dates_studios_and_locations.metricdate,
    dates_studios_and_locations.mbaccountnumber,
    dates_studios_and_locations.studioid,
    dates_studios_and_locations.locationid,
    dates_studios_and_locations.masterlocid,
    dates_studios_and_locations.studioshort,
    dates_studios_and_locations.hasmap as paymentprocessingenabled,
    dates_studios_and_locations.mbfoptin as mbappenabled,
    dates_studios_and_locations.industry as vertical,
    dates_studios_and_locations.sitecreationdate::date as sitecreationdate,
    dates_studios_and_locations.softwarelevel as producttier,
    dates_studios_and_locations.regionid,

-- salesforce_account
    salesforce_account.salesforceid18,

-- tblgenopts
    coalesce(case
                when tblgenopts.enablemarketplacedeals = 1
                then pos.cntintrooffersmarkedpromotedinapp
                else 0
            end, 0) as mpapppromotionssoldcount,
    null as mbappfavoritescount,
    tblgenopts.enablemarketplacedeals,

-- user_agreement
    user_agreement.currentoptinstatus as mpa,

-- classes_and_waitlist
    coalesce(classes_and_waitlist.cntdistinctclasses, 0) as classeswaitlisted,
    coalesce(classes_and_waitlist.cntdistinctclients, 0) as customerswaitlisted,

-- transactions
    case
        when transactions.cnttransactions >= 1
        then 1
        else 0
    end as paymentprocessingutilized,
    coalesce(transactions.cnttransactions, 0) as paymentprocessingqualified,
    coalesce(transactions.cnttransactionsswiped, 0) as paymentsswiped,

-- sms_email
    case 
        when sms_email.acceleratequalified >= 1 
        then 1 
        else 0 
    end as accelerateutilized,
    coalesce(sms_email.acceleratequalified, 0) as acceleratequalified,

-- active_clients
    coalesce(active_clients.cnttotalclients, 0) as totalclients,

-- new_clients
    coalesce(new_clients.cntmbappdiscoveries, 0) as mbappdiscoveries,
    coalesce(new_clients.cntnewclients, 0) as newclients,

-- sales
    case
        when sales.cnttotalsales >= 1
        then 1
        else 0
    end as posutilized,
    coalesce(sales.cnttotalsales, 0) as posqualified,
    -- coalesce(sales.cnttotalsales, 0) end as salescount,
    coalesce(sales.cntgiftcardssold, 0) as giftcardsold,
    coalesce(sales.cntunlimitedssold, 0) as unlimitedsold,
    coalesce(sales.cntmultissold, 0) as multisessionsold,
    coalesce(sales.cntdropinssold, 0) as dropinsold,

-- online_sales
    case
        when online_sales.cntonlinesales >= 1
        then 1
        else 0
    end as onlinesalesutilized,
    coalesce(online_sales.cntonlinesales, 0) as onlinesalesqualified,
    coalesce(online_sales.cntconsumermodeintroofferssold, 0) as consumermodepromosoldcount,
    coalesce(online_sales.cntbrandedwebintroofferssold, 0) as brandedwebpromosoldcount,
    coalesce(online_sales.cntmbappintroofferssold, 0) as mbapppromosoldcount,
    coalesce(online_sales.cntpromotedintroofferssold, 0) as promotedintroofferssold,
    coalesce(online_sales.cntdynamicpricingsold, 0) as cntdynamicpricingsold,
    coalesce(online_sales.cntbrandedappintroofferssold, 0) as brandedapppromosold,

-- class_events_enabled
    coalesce(class_events_enabled.classesenabled, 0) as classbookingenabled,
    coalesce(class_events_enabled.eventsenabled, 0) as bookingeventenabled,
    case
        when class_events_enabled.classonlineenabled = 1
            or appointments_enabled.appointmentonlineenabled = 1
        then 1
        else 0
    end as onlinebookingsenabled,
    case
        when class_events_enabled.maxcapacity > 0
        then (class_events_enabled.totnumvd::float / class_events_enabled.maxcapacity::float)::decimal(10, 2) * 100
        else 0
    end as classcapacitypct,
    coalesce(class_events_enabled.maxcapacity, 0) as classcapacity,
    coalesce(class_events_enabled.totnumvd, 0) as classcapacityutilized,

-- bookings_data
    case
        when bookings_data.classbookings >= 1
        then 1
        else 0
    end as classbookingutilized,
    coalesce(bookings_data.classbookings, 0) as classbookingqualified,
    case
        when bookings_data.appointmentbookings >= 1
        then 1
        else 0
    end as appointmentbookingutilized,
    coalesce(bookings_data.appointmentbookings, 0) as appointmentbookingqualified,
    case
        when bookings_data.eventsbookings >= 1
        then 1
        else 0
    end as bookingeventutilized,
    coalesce(bookings_data.eventsbookings, 0) as bookingeventqualified,
    case
        when bookings_data.cntonlinebookings >= 1
        then 1
        else 0
    end as onlinebookingutilized,
    coalesce(bookings_data.cntonlinebookings, 0) as onlinebookingqualified,
    case
        when bookings_data.mbappbookings >= 1
        then 1
        else 0
    end as mbapputilized,
    coalesce(bookings_data.mbappbookings, 0) as mbappqualified,
    coalesce(bookings_data.mbnetworkbookings, 0) as mbnetworkutilized,
    case 
        when bookings_data.mbnetworkbookings >= 1 
        then 1 
        else 0 
    end as mbnetworkqualified,

    coalesce(bookings_data.cntbookings, 0) as locationbookingscount,
    coalesce(bookings_data.consumermodebookings, 0) as consumermodebookingscount,
    -- coalesce(bookings_data.mbappbookings, 0) as mbappbookingscount,
    coalesce(bookings_data.brandedappbookings, 0) as brandedappbookingscount,
    -- coalesce(bookings_data.mbnetworkbookings, 0) as mbnetworkbookingscount,
    -- coalesce(bookings_data.cntbookings, 0) end as bookingscount,

-- pos
    coalesce(pos.haspos, 0) as posenabled,
    coalesce(pos.onlinepos, 0) as onlinesalesenabled,
    coalesce(pos.haspos, 0) as autopaysenabled,

-- efts
    case 
        when efts.cntautopays >= 1 
        then 1 
        else 0 
    end as autopaysutilized,
    coalesce(efts.cntautopays, 0) as autopaysqualified,
    null as accelerateenabled,

-- appointments_enabled
    coalesce(appointments_enabled.appointmentsenabled, 0) as appointmentbookingenabled,

-- kpitransactions
    coalesce(kpi_transactions.paymentssettledtransactions, 0) as paymentssettledtransactions,

-- kpisales
    coalesce(kpi_sales.revenuesalestotal, 0) as revenuesalestotal,

-- schedule_complete
    coalesce(schedule_complete.schedcompletedclasses, 0) as schedcompletedclasses,
    coalesce(schedule_complete.schedcompletedappointments, 0) as schedcompletedappointments,

-- gmv
    coalesce(gmv.totalgmv, 0) as totalgmv,

    current_timestamp() as etlcreateddatetimeutc

from dates_studios_and_locations

inner join salesforce_account
    on salesforce_account.mindbody_id__c = abs(dates_studios_and_locations.mbaccountnumber)

inner join tblgenopts
    on tblgenopts.studioid = dates_studios_and_locations.studioid

left outer join user_agreement
    on user_agreement.metricdate = dates_studios_and_locations.metricdate
        and user_agreement.studioid = dates_studios_and_locations.studioid

left outer join classes_and_waitlist
    on classes_and_waitlist.metricdate = dates_studios_and_locations.metricdate
        and classes_and_waitlist.studioid = dates_studios_and_locations.studioid
        and classes_and_waitlist.locationid = dates_studios_and_locations.locationid

left outer join transactions
    on transactions.metricdate = dates_studios_and_locations.metricdate
        and transactions.studioid = dates_studios_and_locations.studioid
        and transactions.locationid = dates_studios_and_locations.locationid

left outer join sms_email
    on sms_email.metricdate = dates_studios_and_locations.metricdate
        and sms_email.studioid = dates_studios_and_locations.studioid

left outer join active_clients
    on active_clients.studioid = dates_studios_and_locations.studioid

left outer join new_clients
    on new_clients.metricdate = dates_studios_and_locations.metricdate
        and new_clients.studioid = dates_studios_and_locations.studioid

left outer join sales
    on sales.metricdate = dates_studios_and_locations.metricdate
        and sales.studioid = dates_studios_and_locations.studioid
        and sales.locationid = dates_studios_and_locations.locationid

left outer join online_sales
    on online_sales.metricdate = dates_studios_and_locations.metricdate
        and online_sales.studioid = dates_studios_and_locations.studioid

left outer join class_events_enabled
    on class_events_enabled.metricdate = dates_studios_and_locations.metricdate
        and class_events_enabled.studioid = dates_studios_and_locations.studioid
        and class_events_enabled.locationid = dates_studios_and_locations.locationid

left outer join bookings_data
    on bookings_data.metricdate = dates_studios_and_locations.metricdate
        and bookings_data.studioid = dates_studios_and_locations.studioid
        and bookings_data.locationid = dates_studios_and_locations.locationid

left outer join pos
    on pos.metricdate = dates_studios_and_locations.metricdate
        and pos.studioid = dates_studios_and_locations.studioid

left outer join efts
    on efts.metricdate = dates_studios_and_locations.metricdate
        and efts.studioid = dates_studios_and_locations.studioid
        and efts.locationid = dates_studios_and_locations.locationid

left outer join appointments_enabled
    on appointments_enabled.metricdate = dates_studios_and_locations.metricdate
        and appointments_enabled.studioid = dates_studios_and_locations.studioid

left outer join kpi_transactions
    on kpi_transactions.metricdate = dates_studios_and_locations.metricdate
        and kpi_transactions.studioid = dates_studios_and_locations.studioid
        and kpi_transactions.locationid = dates_studios_and_locations.locationid

left outer join kpi_sales
    on kpi_sales.metricdate = dates_studios_and_locations.metricdate
        and kpi_sales.studioid = dates_studios_and_locations.studioid
        and kpi_sales.locationid = dates_studios_and_locations.locationid

left outer join schedule_complete
    on schedule_complete.metricdate = dates_studios_and_locations.metricdate
        and schedule_complete.studioid = dates_studios_and_locations.studioid
        and schedule_complete.locationid = dates_studios_and_locations.locationid

left outer join gmv
    on gmv.metricdate = dates_studios_and_locations.metricdate
        and gmv.studioid = dates_studios_and_locations.studioid
        and gmv.locationid = dates_studios_and_locations.locationid