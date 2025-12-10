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
        case 
            when player_id = 'a8c55cbe-259a-4e79-8d7d-1595385a1107' then true
            else safe_cast(is_event_player as boolean)
        end as is_event_player,

        case 
            when player_id = 'a8c55cbe-259a-4e79-8d7d-1595385a1107' then true
            else safe_cast(is_og_player as boolean)
        end as is_og_player,

        case 
            when player_id = 'a8c55cbe-259a-4e79-8d7d-1595385a1107' then true
            else safe_cast(is_polish_player as boolean)
        end as is_polish_player,

        case 
            when player_id = 'a8c55cbe-259a-4e79-8d7d-1595385a1107' then true
            else safe_cast(is_tester as boolean)
        end as is_tester,

        case
            when discord_id is not null
            then true
            else false
        end as is_general_player

    from source

)

select * from renamed

where discord_id is not null 
    and player_id is not null