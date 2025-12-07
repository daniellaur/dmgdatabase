with 

source as (

    select * from {{ source('gsheets', 'event_records') }}

),

renamed as (

    select
    
        -- IDs
        safe_cast(record_id as string) as record_id,
        safe_cast(player_id as string) as player_id,

        -- Timestamps
        TIMESTAMP_MILLIS(created_at) as created_at,

        -- Properties
        safe_cast(player_name as string) as player_name,

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
        
        case
        when value = 0 then null
        else safe_cast(value as integer) 
        end as value
        
    from source

)

select * from renamed