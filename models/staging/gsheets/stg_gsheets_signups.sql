with 

source as (

    select * from {{ source('gsheets', 'signups') }}

),

renamed as (

    select
    
        -- IDs
        safe_cast(signup_id as string) as signup_id,
        safe_cast(discord_id as string) as discord_id,
        safe_cast(event_id as string) as event_id,

        -- Timestamps
        timestamp(created_at) as created_at,
        timestamp(extracted_at) as extracted_at,

        -- Properties
        safe_cast(team_with as string) as team_with,
        safe_cast(not_team_with as string) as not_team_with,
        safe_cast(comments as string) as comments

    from source

    where signup_id is not null

)

select * from renamed