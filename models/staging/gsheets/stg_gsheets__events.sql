with 

source as (

    select * from {{ source('gsheets', 'events') }}

),

renamed as (

    select
    
        -- IDs
        safe_cast(event_id as string) as event_id,

        -- Timestamps
        timestamp(created_at) as created_at,
        TIMESTAMP_SECONDS(started_at) as event_started_at,
        timestamp(extracted_at) as extracted_at,

        -- Properties
        safe_cast(event_type as string) as event_type,
        safe_cast(event_number as integer) as event_number

    from source

)

select * from renamed

where event_id is not null