{% macro pivot_columns(values, filter_col, agg_col, agg_func='sum', prefix='', postfix='') %}
{% for value in values %}
{{agg_func}}(case when {{filter_col}}='{{value}}' then {{agg_col}} end) as {{prefix}}{{value}}{{postfix}},
{% endfor %}
{% endmacro %}