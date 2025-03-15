{{
    config(
        materialized='incremental',
    )
}}

select mi.match_id, inning_id, over, ball_in_over, bt.team_id as batter_team_id, bat.player_id as batter_id, bowl.player_id as bowler_id, nons.player_id as non_striker_id, runs_batter, runs_extras, runs_total, dismissal_kind, po.player_id as player_out_id, fn.player_id as fielder_name_id, is_powerplay, extra_type
from {{ ref('int_match_innings') }} bs
left join {{ ref('match_info') }} mi
using(filename)
left join {{ ref('teams') }} bt
on bs.batter_team = bt.team_name
left join {{ ref('players') }} bat
on bs.batter = bat.player_name
left join {{ ref('players') }} bowl
on bs.bowler = bowl.player_name
left join {{ ref('players') }} nons
on bs.non_striker = nons.player_name
left join {{ ref('players') }} po
on bs.player_out = po.player_name
left join {{ ref('players') }} fn
on bs.fielder_name = fn.player_name
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where match_id not in (select distinct match_id from {{ this }})
{% endif %}
order by match_id, inning_id, over, ball_in_over
