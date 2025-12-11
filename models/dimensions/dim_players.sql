with 

players as (

    select * from {{ ref("stg_script__mc_usernames") }}

),

renamed as (

    select

        -- IDs
        players.player_id,
        discord.discord_id,

        -- Properties | Players
        players.player_name,

        -- Properties | Discord
        discord.is_event_player,
        discord.is_og_player,
        discord.is_polish_player,
        discord.is_tester,
        discord.is_general_player

    from players

    left join
    {{ ref('stg_gsheets__discord_users') }} as discord
    on discord.player_id = players.player_id

)

select * from renamed