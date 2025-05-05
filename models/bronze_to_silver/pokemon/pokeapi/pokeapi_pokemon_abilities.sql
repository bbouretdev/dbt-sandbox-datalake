{{ config(materialized = 'ephemeral') }}

WITH abilities_expanded AS (
    SELECT
        id,
        name,
        base_experience,
        height,
        weight,
        is_default,
        "order",
        species->>'name' AS species_name,
        UNNEST(abilities) AS ability_entry
    FROM {{ ref('pokeapi_pokemon_raw') }}
),
split_abilities AS (
    SELECT *,
        ability_entry->>'is_hidden' = 'true' AS is_hidden,
        ability_entry->'ability'->>'name' AS ability_name
    FROM abilities_expanded
),
normal_abilities AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY ability_name) AS ability_rank
    FROM split_abilities
    WHERE is_hidden = false
),
hidden_abilities AS (
    SELECT id, ability_name AS hidden_ability_name
    FROM split_abilities
    WHERE is_hidden = true
)
SELECT 
    n1.id,
    n1.name,
    n1.base_experience,
    n1.height,
    n1.weight,
    n1.is_default,
    n1."order",
    n1.species_name,
    n1.ability_name AS ability_1_name,
    n2.ability_name AS ability_2_name,
    h.hidden_ability_name
FROM normal_abilities n1
LEFT JOIN normal_abilities n2 ON n1.id = n2.id AND n2.ability_rank = 2
LEFT JOIN hidden_abilities h ON n1.id = h.id
WHERE n1.ability_rank = 1