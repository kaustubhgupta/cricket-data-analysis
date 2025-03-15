    with 

source as (

    select * from {{ source('raw', 'players_info') }}

),

renamed as (

    select
        name,
        date_of_birth,
        gender,
        batting_style,
        bowling_style,
        position

    from source

)

select * from renamed
