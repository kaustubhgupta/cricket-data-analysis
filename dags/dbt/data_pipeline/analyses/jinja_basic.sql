-- variable declaration
{%- set newVar = 'kaustubh' %}
-- variable expression
{{- newVar }} 
-- list declaration
{%- set listSkills = ['SQL', 'Python', 'DBT', 'Snowflake'] %}
-- for loop
{%- for i in listSkills %}
{{i}}
{%- endfor %}
-- dictionary
{%- set dict = {
    'A': 'Apple',
    'B': 'Banana'
}%}
{{dict['A']}}