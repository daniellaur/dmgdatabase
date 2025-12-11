with 

source as (

    select * from {{ ref('stg_gsheets__events') }}

),

renamed as (

    select
    
        -- IDs
        event_id,

        -- Timestamps
        created_at,
        event_started_at,
        extracted_at,

        -- Properties
        case
            when event_number = 0 then null
            when event_type = 'main' then concat('DMG ', event_number)
            when event_type = 'og' then concat('OG ', event_number)
            when event_type = 'polish' then concat('PL ', event_number)
            when event_type = 'testing' then concat('T ', event_number)
            when event_type = 'community' then concat('COM ', event_number)
            when event_type = 'private' then concat('PR ', event_number)
            else null
        end as event_name,

        case 
            when event_type = 'main' then 'Main Event'
            when event_type = 'og' then 'OG Event'
            when event_type = 'polish' then 'Polish Event'
            when event_type = 'testing' then 'Testing'
            when event_type = 'community' then 'Community Event'
            when event_type = 'private' then 'Private Events'
            else null
        end as event_type,

        safe_cast(event_number as integer) as event_number,
        
    from source

)

select * from renamed