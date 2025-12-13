with 

signups as (

    select * from {{ ref('sp_signups') }}

),

renamed as (

    select
    
        -- IDs
        signups.signup_id,
        players.player_id,
        signups.discord_id,
        signups.event_id,

        -- Timestamps
        signups.created_at,
        signups.extracted_at,

        -- Properties | Events
        events.event_name,
        events.event_type,
        events.event_number,

        -- Properties | Players
        players.player_name,

        -- Properties | Signups
        signups.team_with,
        signups.not_team_with,
        signups.comments

    from signups

    left join
    {{ ref("dim_players") }} as players
    on players.discord_id = signups.discord_id

    left join
    {{ ref("dim_events") }} as events
    on events.event_id = signups.event_id

)

select * from renamed