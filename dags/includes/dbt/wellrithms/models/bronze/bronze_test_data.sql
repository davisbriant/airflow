with source as (
	select 
	"$path" as _path,
	cast(split_part("$path", '/', 8) as timestamp) AS converted_at,
	json_payload
	from {{ source('wellrithms_testdata_json', 'sources-test_datajson') }}
)

select 
*
from 
source