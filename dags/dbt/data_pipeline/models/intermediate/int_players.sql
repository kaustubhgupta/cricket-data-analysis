with cte as (
select 
f0.key as team_name,
f1.value::varchar as team_player,
from {{ ref('stg_raw__cricket_raw_data') }} bs,
lateral flatten (input=>bs.json_data:info:players) f0,
lateral flatten (input=>f0.value) f1
group by all)

select * exclude(name)
from cte ct
left join {{ ref('stg_raw__players_info') }} pi
on ct.team_player = pi.name