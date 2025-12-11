select

    -- IDs
    player_id,
    discord_id,

    -- Properties
    player_name,
    event_type,
    
    -- Measures
    average_points,
    average_placement,
    events_played_3m,
    events_played_6m

from {{ ref('agg_teammaker_stats') }}