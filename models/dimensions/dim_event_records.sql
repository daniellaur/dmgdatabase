with 

source as (

    select * from {{ ref("stg_gsheets__event_records") }}

),

renamed as (

    select

        -- IDs
        source.record_id,
        source.player_id,
        source.event_id,

        -- Timestamps
        source.created_at,

        -- Properties
        source.player_team,
        source.event_type,
        source.event_number,
        source.objective,
        source.value,

        case
            when left(source.objective, 2) = 'sb' then 'Sky Battle'
            when left(source.objective, 2) = 'gr' then 'Gold Rush'
            when left(source.objective, 2) = 'sc' then 'Super Clash'
            when left(source.objective, 2) = 'cs' then 'Cow Strike'
            when left(source.objective, 2) = 'sf' then 'Skyfall'
            when left(source.objective, 2) = 'hs' then 'Housing'
            when left(source.objective, 2) = 'tr' then 'Theme Race'
            when left(source.objective, 2) = 'bd' then 'Block Dash'
            when left(source.objective, 2) = 'bg' then 'Bingo'
            when left(source.objective, 2) = 'fd' then 'General'
            when left(source.objective, 3) = 'wins' then 'General'
            when left(source.objective, 6) = 'points' then 'General'
            else null
        end as game_name,

        case
            when right(source.objective, 3) = 'wins' then 'Wins'
            when right(source.objective, 14) = 'participations' then 'Participations'
            when right(source.objective, 6) = 'points' then 'Points'
            when right(source.objective, 5) = 'kills' then 'Eliminations'
            when right(source.objective, 6) = 'rounds' then 'Rounds Survived'
            when right(source.objective, 6) = 'blocks' then 'Blocks Mined'
            when right(source.objective, 6) = 'hearts' then 'Damage Dealt'
            when right(source.objective, 11) = 'checkpoint1' then 'Furthest After 6 Min'
            when right(source.objective, 11) = 'checkpoint2' then 'Furthest Stage'
            when right(source.objective, 10) = 'challenges' then 'Challenge Completions'
            when right(source.objective, 14) = 'challenges_1st' then '1st Challenge Completions'
            when right(source.objective, 8) = 'laps_1st' then 'First Place Laps'
            when right(source.objective, 12) = 'finished_1st' then 'First Place Finishes'
            else null
        end as game_objective,

        dense_rank() over (
            partition by source.event_id, source.objective
            order by value desc
        ) as player_placement

    from source

)

select * from renamed