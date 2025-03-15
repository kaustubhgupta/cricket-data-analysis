{{
    config(
        materialized='incremental',
    )
}}

select raw.seq_teams.nextval as team_id, team_type, team team_name
from {{ ref('int_team') }}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where team_name not in (select team_name from {{this}})
{% endif %}