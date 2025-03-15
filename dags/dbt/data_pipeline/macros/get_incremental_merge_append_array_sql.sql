{% macro get_incremental_merge_append_array_sql(arg_dict) %}
  {%- set merge_append_array_column = config.get('merge_append_array_column') -%}

  {% do return(custom(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["unique_key"], arg_dict["dest_columns"], merge_append_array_column)) %}

{% endmacro %}


{% macro custom(target, source_t, unique_key, dest_columns, merge_append_array_column) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}


merge into {{ target }} as target
using {{ source_t }} as source_t
on target.{{ unique_key }} = source_t.{{ unique_key }}

when matched then update set
    {{ merge_append_array_column }} = array_distinct(array_cat(target.{{ merge_append_array_column }}, source_t.{{merge_append_array_column}}))

when not matched then insert ({{ dest_cols_csv }}) values ({{ dest_cols_csv }})

{% endmacro %}
