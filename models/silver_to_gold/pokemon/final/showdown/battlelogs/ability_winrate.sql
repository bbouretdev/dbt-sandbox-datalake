with base as (

    -- On ne garde qu'un seul enregistrement par Pokémon joué dans chaque match
    select distinct
        roomid,
        player,
        winner,
        species,
        ability
    from {{ ref('battle_team_exploded') }}
    where ability is not null and ability != ''

),

ability_stats as (

    -- Statistiques de winrate par ability
    select
        ability,
        count(*) as total_appearances,
        sum(case when player = winner then 1 else 0 end) as total_wins,
        round(100.0 * sum(case when player = winner then 1 else 0 end) / count(*), 2) as winrate_percent
    from base
    group by ability
    order by winrate_percent desc

)

select * from ability_stats