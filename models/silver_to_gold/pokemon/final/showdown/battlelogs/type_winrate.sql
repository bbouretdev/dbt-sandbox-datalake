with base as (

    -- On lie chaque Pokémon à ses types (chaque ligne = un Pokémon dans une équipe)
    select distinct
        b.roomid,
        b.player,
        b.winner,
        p.type_1,
        p.type_2
    from {{ ref('battle_team_exploded') }} b
    left join read_parquet('s3://sandbox-datalake-gold/pokemon/showdown/dav/latest/playable_pokemon.parquet') p
        on lower(b.species) = lower(p.name)

),

all_types as (

    -- On transforme type_1 et type_2 en lignes séparées
    select roomid, player, winner, type_1 as type from base
    where type_1 is not null and type_1 != ''
    
    union all

    select roomid, player, winner, type_2 as type from base
    where type_2 is not null and type_2 != ''

),

type_stats as (

    -- On compte le nombre total de fois qu'un type apparaît dans une équipe
    -- et combien de fois ce type était dans l'équipe gagnante
    select
        type,
        count(*) as total_appearances,
        sum(case when player = winner then 1 else 0 end) as total_wins,
        round(100.0 * sum(case when player = winner then 1 else 0 end) / count(*), 2) as winrate_percent
    from all_types
    group by type
    order by winrate_percent desc

)

select * from type_stats