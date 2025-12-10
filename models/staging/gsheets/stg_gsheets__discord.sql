with 

source as (

    select * from {{ source('gsheets', 'discord') }}

),

renamed as (

    select
    
        -- IDs
        safe_cast(discord_id as string) as discord_id,
        safe_cast(player_id as string) as player_id,

        -- Timestamps
        timestamp(updated_at) as updated_at,
        timestamp(extracted_at) as extracted_at,

        -- Properties
        safe_cast(is_event_player as boolean) as is_event_player,
        safe_cast(is_og_player as boolean) as is_og_player,
        safe_cast(is_polish_player as boolean) as is_polish_player,
        safe_cast(is_tester as boolean) as is_tester,
        case
            when is_event_player is false
            and is_og_player is false
            and is_polish_player is false
            and is_tester is false
            then true
            else false
        end as is_general_player

    from source

)

select * from renamed

where discord_id is not null and player_id is not null