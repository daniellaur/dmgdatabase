with 

source as (

    select * from {{ ref("stg_script__current_player_name") }}

),

renamed as (

    select

        -- IDs
        source.player_id,

        -- Properties
        source.player_name

    from source

)

select * from renamed