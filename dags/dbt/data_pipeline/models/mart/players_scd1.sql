{{
    config(
        materialized='incremental',
        unique_key='player_name',
        incremental_strategy='merge',
        merge_update_columns=['teams']
    )
}}

select raw.seq_players.nextval as player_id, *
from (
select team_player player_name, array_agg(distinct t.team_id) teams
from {{ ref('int_players') }} sp
left join {{ ref('teams') }} t
using(team_name)
group by all)