select

    -- IDs
    record_id,

    -- Timestamps
    created_at,

    -- Properties
    event_name,
    event_type,
    player_name,
    player_objective,
    player_value,
    player_placement,
    game_name,
    game_objective,

from {{ ref('fct_mc_records') }}