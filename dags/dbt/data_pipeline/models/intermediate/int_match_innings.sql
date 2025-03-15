with 
base_inning_info as (
select 
filename, 
inning_lvl.index+1::int as inning_id,
inning_lvl.value:team:: varchar as batter_team,
over_lvl.index+1::int as over,
bowl_lvl.index+1::int as ball_in_over,
bowl_lvl.value:batter::varchar as batter,
bowl_lvl.value:bowler::varchar as bowler,
bowl_lvl.value:non_striker::varchar as non_striker,
bowl_lvl.value:runs:batter::int as runs_batter,
bowl_lvl.value:runs:extras::int as runs_extras,
bowl_lvl.value:runs:total::int as runs_total,
bowl_lvl.value:wickets[0]:kind::varchar as DISMISSAL_KIND,
bowl_lvl.value:wickets[0]:player_out::varchar as player_out,
bowl_lvl.value:wickets[0]:fielders[0]:name::varchar as fielder_name,
bowl_lvl.value:extras::variant as raw_extra_type,
case when over between coalesce(ceil(inning_lvl.value:powerplays[0]:from), dp.start_over) and coalesce(ceil(inning_lvl.value:powerplays[0]:tp), dp.end_over) then 1 else 0 end as is_powerplay
from {{ ref('stg_raw__cricket_raw_data') }} bs
left join {{ ref('default_powerplays') }} dp 
on bs.json_data:info:match_type::varchar = dp.match_type,
lateral flatten (input=>bs.json_data:innings) inning_lvl,
lateral flatten (input=>inning_lvl.value:overs) over_lvl,
lateral flatten (input=>over_lvl.value:deliveries) bowl_lvl
)

, extra_type_cte as (
select 
filename, inning_id, over, ball_in_over, ext_type.key as extra_type
from base_inning_info bs,
lateral flatten (input=>bs.raw_extra_type) ext_type
)

select bs.* exclude(raw_extra_type), ext.extra_type
from base_inning_info bs
left join extra_type_cte ext
using (filename, inning_id, over, ball_in_over)