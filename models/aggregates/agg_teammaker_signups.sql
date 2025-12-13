with 

source as (

    select * from {{ ref('fct_signups') }}

),

renamed as (

    select

        -- IDs
        signup_id,
        player_id,
        discord_id,
        event_id,

        -- Properties | Events
        event_name,
        event_type,
        event_number,

        -- Properties | Players
        player_name,

        -- Properties | Signups
        team_with,
        not_team_with,
        comments

    from source

),

ranked as (

    select

        *,

        dense_rank() over (
            partition by event_type
            order by event_number desc
        ) as rank

    from renamed

)

select * from ranked

where rank = 1