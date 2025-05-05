{{ config(materialized = 'ephemeral') }}

SELECT
    id,
    types[1]->'type'->>'name' AS type_1,
    types[2]->'type'->>'name' AS type_2,
    stats[1]->>'base_stat' AS "hp",
    stats[2]->>'base_stat' AS "attack",
    stats[3]->>'base_stat' AS "defense",
    stats[4]->>'base_stat' AS "special-attack",
    stats[5]->>'base_stat' AS "special-defense",
    stats[6]->>'base_stat' AS "speed"
FROM {{ ref('pokeapi_pokemon_raw') }}