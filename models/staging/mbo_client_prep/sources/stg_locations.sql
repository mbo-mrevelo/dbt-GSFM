select
    try_to_numeric(studioid) as studioid,
	try_to_numeric(locationid) as locationid,
	try_to_numeric(mbaccountnumber) as mbaccountnumber,
	try_to_numeric(masterlocid) as masterlocid,
	case
		when (mid is not null and mid != '') or (tid is not null and tid != '') then 1
		else 0
	end as hasmap

from {{ source('mbo_client_prep', 'wsmaster_location') }}

where locationid != 98
	and active = 1