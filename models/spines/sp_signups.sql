with 

source as (

    select * from {{ ref('stg_gsheets__signups') }}

),

renamed as (

    select
    
        -- IDs
        signup_id,
        discord_id,
        event_id,

        -- Timestamps
        created_at,
        extracted_at,

        -- Properties
        team_with,
        not_team_with,
        comments

    from source

)

select * from renamed