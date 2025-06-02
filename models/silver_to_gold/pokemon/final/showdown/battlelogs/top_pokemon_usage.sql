with usage_base as (

    -- On ne garde qu'une fois chaque Pokémon utilisé dans chaque match
    select distinct
        roomid,
        player,
        species
    from {{ ref('battle_team_exploded') }}

),

pokemon_usage as (

    select
        species as pokemon,
        count(*) as usage_count
    from usage_base
    group by species
    order by usage_count desc
    limit 100

)

select * from pokemon_usage