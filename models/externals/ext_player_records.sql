select

    -- IDs
    record_id,
    player_id,
    event_id,

    -- Timestamps
    created_at,

    -- Properties
    player_name,
    player_team,
    event_type,
    event_number,
    objective,
    value,
    game_name,
    game_objective

from {{ ref('fct_player_records') }}