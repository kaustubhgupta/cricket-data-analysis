{{
    config(
        materialized='incremental',
    )
}}

select raw.seq_matches.nextval as match_id, match_type, gender, season, city, venue, event_type, event_name, win_t.team_id as winner_team_id, toss_decision, toss_win.team_id as toss_winner_id, t1.team_id as team1_name_id, t2.team_id as team2_name_id, match_start_date, pom.player_id as player_of_match_id, win_type, win_margin, filename
from {{ ref('int_match_info') }} bs
left join {{ ref('teams') }} win_t
on bs.winner_name = win_t.team_name
left join {{ ref('teams') }} toss_win
on bs.toss_winner = toss_win.team_name
left join {{ ref('teams') }} t1
on bs.team1_name = t1.team_name
left join {{ ref('teams') }} t2
on bs.team2_name = t2.team_name
left join {{ ref('players') }} pom
on bs.player_of_match = pom.player_name
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where filename not in (select filename from {{this}})
{% endif %}

