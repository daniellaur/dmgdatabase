with 

source as (

    select * from {{ ref('fct_mc_records') }}

),

renamed as (

    select
        -- IDs
        player_id,
        discord_id,

        -- Properties
        player_name,
        event_type,

        -- Measures
        cast(round(avg(case when player_objective = 'points' then player_value end), 0) as int64) as average_points,
        cast(round(avg(case when player_objective = 'points' then player_placement end), 0) as int64) as average_placement,

        count(
            distinct case
                when date(event_started_at) >= date_sub(current_date(), interval 3 month)
                then event_id
            end
        ) as events_played_3m,

        count(
            distinct case
                when date(event_started_at) >= date_sub(current_date(), interval 6 month)
                then event_id
            end
        ) as events_played_6m

    from source

    group by
        player_id,
        discord_id,
        player_name,
        event_type

)

select * from renamed