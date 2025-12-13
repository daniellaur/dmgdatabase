with 

source as (

    select * from {{ ref('fct_mc_records') }}

    where game_objective = 'Points'

),

renamed as (

    select
        -- IDs
        event_id,

        -- Timestamps
        date(created_at) as created_at_date,

        -- Properties
        event_name,
        event_type,
        player_team as team_name,
        
        case
        when game_name = 'General' then null
        else game_name
        end as game_name,

        -- Measures
        sum(player_value) as team_value

    from source

    where game_name is not null

    group by
        event_id,
        created_at_date,
        event_name,
        event_type,
        team_name,
        game_name

)

select * from renamed