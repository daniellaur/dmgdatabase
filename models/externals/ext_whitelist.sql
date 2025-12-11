select

    -- IDs
    player_id,

    -- Properties
    is_event_player,
    is_og_player,
    is_polish_player,
    is_tester,
    is_general_player

from {{ ref('dim_players') }}