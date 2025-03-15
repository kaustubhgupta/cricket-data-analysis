select *
from {{ ref('int_match_innings') }}
where runs_batter < 0