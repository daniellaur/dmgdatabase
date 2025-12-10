with 

source as (

    select * from {{ source('gsheets', 'event_records') }}

),

renamed as (

    select
    
        -- IDs
        safe_cast(record_id as string) as record_id,
        safe_cast(player_id as string) as player_id,

        case
            when event_number is null or event_number = 0 then null
            when event_type = 'main' then concat('DMG ', event_number)
            when event_type = 'og' then concat('OG ', event_number)
            when event_type = 'poland' then concat('PL ', event_number)
            when event_type = 'testing' then concat('T ', event_number)
            when event_type = 'community' then concat('COM ', event_number)
            when event_type = 'private' then concat('PR ', event_number)
            else null
        end as event_id,

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

        case 
            when event_type = 'main' then 'Main Event'
            when event_type = 'og' then 'OG Event'
            when event_type = 'poland' then 'Polish Event'
            when event_type = 'testing' then 'Testing'
            when event_type = 'community' then 'Community Event'
            when event_type = 'private' then 'Private Events'
            else null
        end as event_type,

        safe_cast(event_number as integer) as event_number,
        safe_cast(objective as string) as objective,
        safe_cast(value as integer) as value,
        
    from source

)

select * from renamed

where value > 0