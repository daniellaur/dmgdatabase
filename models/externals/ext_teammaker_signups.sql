select

    -- IDs
    signup_id,
    player_id,
    discord_id,
    event_id,

    -- Properties | Events
    event_name,
    event_type,
    event_number,

    -- Properties | Players
    player_name,

    -- Properties | Signups
    team_with,
    not_team_with,
    comments

from {{ ref('agg_teammaker_signups') }}