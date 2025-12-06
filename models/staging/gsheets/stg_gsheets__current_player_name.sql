with 

source as (

    select * from {{ source('dmgdatabase', 'current_player_name') }}

),

renamed as (

    select
    
        -- IDs
        safe_cast(player_id as string) as player_id,

        -- Properties
        safe_cast(player_name as string) as player_name,

    from source

)

select * from renamed