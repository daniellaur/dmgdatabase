select

    -- IDs
    record_id,
    event_id,

    -- Timestamps
    created_at,

    -- Properties
    player_name,
    event_type,
    objective,
    value,
    game_name,
    game_objective,
    player_placement

from {{ ref('fct_player_records') }}