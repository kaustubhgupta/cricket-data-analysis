{{
    config(
        materialized='incremental',
        transient=false,
        pre_hook="update {{this}} set record_type='old'",
        post_hook="delete from {{this}} where record_type='old'"

    )
}}

select raw_json as json_data, 
filename, 
load_date as etl_load_timestamp,
'new' as record_type
from {{ source('raw', 'raw_data') }}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where load_date > (select max(etl_load_timestamp) from {{ this }}) 
{% endif %}