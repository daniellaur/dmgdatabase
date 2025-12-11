with 

source as (

    select * from {{ source('script', 'mc_usernames') }}

),

renamed as (

    select
    
        -- IDs
        safe_cast(player_id as string) as player_id,

        -- Timestamps
        timestamp(updated_at) as updated_at,

        -- Properties
        safe_cast(player_name as string) as player_name

    from source

)

select * from renamed