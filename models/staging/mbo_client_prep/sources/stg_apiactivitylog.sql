--('%engage%', '%express%', '%hathway%', '%ios%', '%connect%', '%mindbody%', 'healcodetest', 'fitnessmobileapps', 'testclient', 'mattholdenapicreds', 'fitmetrix', 'lymber')
    
select
    logtimestamp::date as metricdate,
    studioid,
    case
        when lower(sourcename) not in ('healcodedata', 'mbo.brandedweb')
        then 1
        else 0
    end as apipartners,
    case
        when lower(sourcename) in ('healcodedata', 'mbo.brandedweb')
        then 1
        else 0
    end as brandedweb

from {{ source('mbo_client_prep', 'apiactivitylog') }}

where logtimestamp::date >= {{ var('start_date') }}
    and logtimestamp::date < {{ var('end_date') }}
    and lower(sourcename) not in ('healcodetest','ios engage','android engage','fitnessmobileapps','mboengagedroidv1','mboengageiosv1',
                                'ios connect','android connect','testclient','mbokiosk','mattholdenapicreds','mindbody kiosk ios','mindbody core software',
                                'hathway','hathwaymbo','hathwayios','hathwaydroid','hathwayipad','hathwayinc','expressios','androidexpress',
                                'mindbody express android','mboexpressdroidv1','mboexpressiosv1', 'mboexpressdroidoauth','mboexpressiosoauth',
                                'fitnessmobileapps', 'hathway', 'mbconnect', 'hathwaymbo', 'hathwayios', 'hathwaydroid', 'hathwayipad', 'hathwayinc', 
                                'expressios', 'androidexpress', 'mboengageiosv1', 'mboengagedroidv1', 'mboexpressiosv1', 'mboexpressdroidv1', 'mboengageiosv2', 
                                'mboengagedroidv2', 'cybersecexpress', 'engageandroiddevelopment', 'engagesoapapps', 'mboexpressdroidoauth', 'mboexpressiosoauth',
                                'mboexpressiosoath','mboengageiosv1','mboexpressdroidoauth','mbokiosk','fitmetrix','lymber','engagesoapapps')