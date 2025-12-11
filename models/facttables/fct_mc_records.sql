with 

mc_records as (

    select * from {{ ref('sp_mc_records') }}

),

renamed as (

    select

        -- IDs
        mc_records.record_id,
        mc_records.player_id,
        players.discord_id,
        mc_records.event_id,

        -- Timestamps
        mc_records.created_at,
        events.event_started_at,

        -- Properties | Events
        events.event_name,
        events.event_type,
        events.event_number,

        -- Properties | Players
        players.player_name,
        players.is_event_player,
        players.is_og_player,
        players.is_polish_player,
        players.is_tester,
        players.is_general_player,

        -- Properties | Mc Records
        mc_records.player_team,
        mc_records.player_objective,
        mc_records.player_value,
        mc_records.player_placement,
        mc_records.game_name,
        mc_records.game_objective

    from mc_records

    left join
    {{ ref("dim_players") }} as players
    on players.player_id = mc_records.player_id

    left join
    {{ ref("dim_events") }} as events
    on events.event_id = mc_records.event_id

)

select * from renamed