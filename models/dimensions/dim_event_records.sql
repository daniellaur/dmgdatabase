with 

event_records as (

    select * from {{ ref("stg_gsheets__event_records") }}

),

renamed as (

    select

        -- IDs
        event_records.record_id,
        event_records.player_id,

        -- Timestamps
        event_records.created_at,

        -- Properties
        current_player_name.player_name,
        event_records.player_team,
        event_records.event_type,
        event_records.event_number,
        event_records.objective,
        event_records.value

    from event_records

    left join
    {{ ref("stg_gsheets__current_player_name") }} as current_player_name
    on current_player_name.player_id = event_records.player_id

)

select * from renamed