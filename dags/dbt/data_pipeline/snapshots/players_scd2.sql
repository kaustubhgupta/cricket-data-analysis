{% snapshot players_scd2 %}
{{
    config(
        unique_key='player_name',
        strategy='check',
        target_schema='tranformed_data',
        check_cols=['teams']
    )
}}

select raw.seq_players.nextval as player_id, *
from (
select team_player player_name, array_agg(distinct t.team_id) teams
from {{ ref('int_players') }} sp
left join {{ ref('teams') }} t
using(team_name)
group by all)
 {% endsnapshot %}

