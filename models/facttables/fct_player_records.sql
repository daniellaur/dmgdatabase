with 

event_records as (

    select * from {{ ref('dim_event_records') }}

),

renamed as (

    select

        -- IDs
        event_records.record_id,
        event_records.player_id,

        case
        when event_number is null or event_number = 0 then null
        when event_type = 'Main Event' then concat('DMG ', event_number)
        when event_type = 'OG Event' then concat('OG ', event_number)
        when event_type = 'Polish Event' then concat('PL ', event_number)
        when event_type = 'Testing' then concat('T ', event_number)
        when event_type = 'Community Event' then concat('COM ', event_number)
        when event_type = 'Private Event' then concat('PR ', event_number)
        else null
        end as event_id,

        -- Timestamps
        event_records.created_at,

        -- Properties
        event_records.player_name,
        event_records.player_team,
        event_records.event_type,
        event_records.event_number,
        event_records.objective,
        event_records.value,

        case
        when left(objective, 2) = 'sb' then 'Sky Battle'
        when left(objective, 2) = 'gr' then 'Gold Rush'
        when left(objective, 2) = 'sc' then 'Super Clash'
        when left(objective, 2) = 'cs' then 'Cow Strike'
        when left(objective, 2) = 'sf' then 'Skyfall'
        when left(objective, 2) = 'hs' then 'Housing'
        when left(objective, 2) = 'tr' then 'Theme Race'
        when left(objective, 2) = 'bd' then 'Block Dash'
        when left(objective, 2) = 'bg' then 'Bingo'
        when left(objective, 2) = 'fd' then 'General'
        when left(objective, 3) = 'win' then 'General'
        when left(objective, 6) = 'points' then 'General'
        else null
        end as game_name,

        case
        when right(objective, 3) = 'win' then 'Wins'
        when right(objective, 14) = 'participations' then 'Participations'
        when right(objective, 6) = 'points' then 'Points'
        when right(objective, 5) = 'kills' then 'Eliminations'
        when right(objective, 6) = 'rounds' then 'Rounds Survived'
        when right(objective, 6) = 'blocks' then 'Blocks Mined'
        when right(objective, 6) = 'hearts' then 'Damage Dealt'
        when right(objective, 11) = 'checkpoint1' then 'Furthest After 6 Min'
        when right(objective, 11) = 'checkpoint2' then 'Furthest Stage'
        when right(objective, 10) = 'challenges' then 'Challenge Completions'
        when right(objective, 14) = 'challenges_1st' then '1st Challenge Completions'
        when right(objective, 8) = 'laps_1st' then 'First Place Laps'
        when right(objective, 12) = 'finished_1st' then 'First Place Finishes'
        else null
        end as game_objective

    from event_records

)

select * from renamed