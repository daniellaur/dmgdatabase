select

    -- IDs
    event_id,

    -- Timestamps
    created_at_date,

    -- Properties
    event_name,
    team_name,
    game_name,

    -- Measures
    team_value

from {{ ref('agg_team_overview') }}