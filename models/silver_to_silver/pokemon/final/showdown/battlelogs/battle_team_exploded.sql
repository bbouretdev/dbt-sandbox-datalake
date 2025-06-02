with base as (

    select *
    from read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/battlelogs/daily_flat/*.parquet')

),

exploded_p1 as (

    select
        roomid,
        log_date,
        format,
        p1 as player,
        'p1' as player_slot,
        winner,
        p1team as team
    from base

),

exploded_p2 as (

    select
        roomid,
        log_date,
        format,
        p2 as player,
        'p2' as player_slot,
        winner,
        p2team as team
    from base

),

combined as (

    select * from exploded_p1
    union all
    select * from exploded_p2

),

unnested_team as (

    -- Une ligne par Pokémon dans une équipe
    select
        *,
        unnest(team) as pokemon
    from combined

)

select
    roomid,
    log_date,
    format,
    player,
    player_slot,
    winner,
    case when winner = player_slot then 1 else 0 end as is_victory,
    pokemon.name,
    pokemon.species,
    pokemon.ability,
    pokemon.item,
    pokemon.moves,
    pokemon.nature,
    pokemon.gender,
    pokemon.happiness,
    pokemon.hpType,
    pokemon.pokeball,
    pokemon.gigantamax,
    pokemon.dynamaxLevel,
    pokemon.teraType,
    pokemon.level,
    pokemon.ivs,
    pokemon.evs,
    concat(roomid, '_', player, '_', pokemon.species) as roomid_pokemon

from unnested_team