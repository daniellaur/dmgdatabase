with 

source as (

    select * from {{ source('gsheets', 'mc_records') }}

),

renamed as (

    select
    
        -- IDs
        safe_cast(record_id as string) as record_id,
        safe_cast(player_id as string) as player_id,
        safe_cast(event_id as string) as event_id,

        -- Timestamps
        TIMESTAMP_MILLIS(created_at) as created_at,
        timestamp(extracted_at) as extracted_at,

        -- Properties
        case 
            when player_team = 1 then 'Red'
            when player_team = 2 then 'Orange'
            when player_team = 3 then 'Yellow'
            when player_team = 4 then 'Lime'
            when player_team = 5 then 'Green'
            when player_team = 6 then 'Blue'
            when player_team = 7 then 'Purple'
            when player_team = 8 then 'Pink'
            else null
        end as player_team,

        safe_cast(player_objective as string) as player_objective,
        safe_cast(player_value as integer) as player_value
        
    from source

    where player_value > 0

)

select * from renamed