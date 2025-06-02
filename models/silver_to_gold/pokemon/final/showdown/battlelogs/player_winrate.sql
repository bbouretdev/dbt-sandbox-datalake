with base as (

    -- On récupère les colonnes utiles une seule fois par joueur et match
    select distinct
        roomid,
        player,
        player_slot,
        winner
    from {{ ref('battle_team_exploded') }}

),

winrate as (

    select
        player,
        count(*) as total_matches,
        sum(case when winner = player then 1 else 0 end) as total_wins,
        round(100.0 * sum(case when winner = player then 1 else 0 end) / count(*), 2) as winrate_percent
    from base
    group by player
    order by winrate_percent desc

)

select * from winrate