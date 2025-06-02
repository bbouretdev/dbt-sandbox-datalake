with base as (

    -- On récupère les infos d'équipe par joueur et match
    select distinct
        roomid,
        player,
        species,
        winner
    from {{ ref('battle_team_exploded') }}

),

pokemon_winrate as (

    select
        species as pokemon,
        count(*) as total_appearances,
        sum(case when player = winner then 1 else 0 end) as total_wins,
        round(100.0 * sum(case when player = winner then 1 else 0 end) / count(*), 2) as winrate_percent
    from base
    group by species
    order by winrate_percent desc

)

select * from pokemon_winrate