{% set table_name = ref('int_match_innings') %}

{% set result = run_query("SELECT DISTINCT extra_type FROM " ~ table_name ~ " WHERE extra_type is not null") %}

{% if result and result.columns[0].values() %}
  {% set extra_types = result.columns[0].values() %}
{% else %}
  {% set extra_types = [] %}
{% endif %}


select filename
{% for extra in extra_types%}
, sum(case when extra_type='{{extra}}' then runs_extras end) as {{extra}}_runs
{% endfor %}
from {{ table_name }}
group by all