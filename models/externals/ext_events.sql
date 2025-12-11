select

    -- IDs
    event_id,

    -- Properties
    event_name_mc,
    event_suffix_mc

from {{ ref('dim_events') }}