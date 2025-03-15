with base_match_info as (
select 
filename,
bs.json_data:info:match_type::varchar as match_type,
bs.json_data:info:gender::varchar as gender,
bs.json_data:info:season::varchar as season,
bs.json_data:info:city::varchar as city,
bs.json_data:info:venue::varchar as venue,
bs.json_data:info:team_type::varchar as event_type,
bs.json_data:info:event:name::varchar as event_name,
bs.json_data:info:outcome:winner::varchar as winner_name,
bs.json_data:info:toss:decision::varchar as toss_decision,
bs.json_data:info:toss:winner::varchar as toss_winner,
bs.json_data:info:teams[0]::varchar as team1_name,
bs.json_data:info:teams[1]::varchar as team2_name,
bs.json_data:info:dates[0]::date as match_start_date,
bs.json_data:info:player_of_match[0]::string as player_of_match,
coalesce(bs.json_data:info:outcome:by::variant, {'tie': 0}) as raw_outcome
from {{ ref('stg_raw__cricket_raw_data') }} bs
)

, with_outcome_cte as (
select 
bs.* exclude(raw_outcome),
ot.key as win_type, 
ot.value as win_margin
from base_match_info bs,
lateral flatten (input=>bs.raw_outcome) ot
where win_type <> 'innings'
)
select * from with_outcome_cte