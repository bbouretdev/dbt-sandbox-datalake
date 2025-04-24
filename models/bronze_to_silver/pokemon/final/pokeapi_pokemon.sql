{{ config(materialized = 'table') }}

SELECT
    a.id,
    a.name,
    a.base_experience,
    a.height,
    a.weight,
    a.is_default,
    a."order",
    a.species_name,
    t.type_1,
    t.type_2,
    t.hp,
    t.attack,
    t.defense,
    t."special-attack",
    t."special-defense",
    t.speed,
    a.ability_1_name,
    a.ability_2_name,
    a.hidden_ability_name
FROM {{ ref('pokeapi_pokemon_abilities') }} a
JOIN {{ ref('pokeapi_pokemon_types_stats') }} t ON a.id = t.id
ORDER BY a.id