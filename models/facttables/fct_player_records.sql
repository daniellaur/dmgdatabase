with 

event_records as (

    select * from {{ ref('dim_event_records') }}

),

renamed as (

    select

        -- IDs
        event_records.record_id,
        event_records.player_id,
        event_records.event_id,

        -- Timestamps
        event_records.created_at,

        -- Properties | Players
        players.player_name,

        -- Properties | Event Records
        event_records.player_team,
        event_records.event_type,
        event_records.event_number,
        event_records.objective,
        event_records.value,
        event_records.game_name,
        event_records.game_objective,
        event_records.player_placement

    from event_records

    left join
    {{ ref("dim_players") }} as players
    on players.player_id = event_records.player_id

)

select * from renamed