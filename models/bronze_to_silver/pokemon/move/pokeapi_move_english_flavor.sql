{{ config(materialized = 'ephemeral') }}

WITH flattened AS (
    SELECT
        id,
        name,
        accuracy,
        power,
        pp,
        priority,
        damage_class->>'name' AS damage_class,
        target->>'name' AS target,
        type->>'name' AS move_type,
        generation->>'name' AS generation,
        UNNEST(flavor_text_entries) AS flavor_entry
    FROM {{ ref('pokeapi_move_raw') }}
)

SELECT
    id,
    name,
    accuracy,
    power,
    pp,
    priority,
    damage_class,
    target,
    move_type,
    generation,
    flavor_entry
FROM flattened