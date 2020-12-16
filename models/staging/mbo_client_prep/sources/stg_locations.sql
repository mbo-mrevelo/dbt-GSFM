select
    studioid,
	locationid,
	mbaccountnumber,
	masterlocid,
	case
		when (mid is not null and mid != '') or (tid is not null and tid != '') then 1
		else 0
	end as hasmap

from {{ source('mbo_client_prep', 'wsmaster_location') }}

where locationid != 98
	and active = 1