select 
bs.team_type,
f.value::varchar as team
from (
select 
json_data:info:team_type::varchar as team_type, 
json_data:info:teams::array as teams, 
from {{ ref('stg_raw__cricket_raw_data') }}) bs,
lateral flatten (input=>bs.teams) f
group by all