{{ config(materialized = 'table') }}

WITH base_chain AS (
    SELECT
        id AS evolution_chain_id,
        chain->'species'->>'name' AS base_pokemon,
        chain->'evolves_to' AS stage_1_json
    FROM {{ ref('evolution_chain_raw') }}
),

first_stage AS (
    SELECT
        evolution_chain_id,
        base_pokemon,
        evo_1->'species'->>'name' AS evolved_pokemon,
        1 AS stage,
        evo_1->'evolves_to' AS stage_2_json
    FROM base_chain,
    UNNEST(stage_1_json) AS evo_1
),

second_stage AS (
    SELECT
        evolution_chain_id,
        evolved_pokemon AS base_pokemon,
        evo_2->'species'->>'name' AS evolved_pokemon,
        2 AS stage,
        evo_2->'evolves_to' AS stage_3_json
    FROM first_stage,
    UNNEST(stage_2_json) AS evo_2
),

third_stage AS (
    SELECT
        evolution_chain_id,
        evolved_pokemon AS base_pokemon,
        evo_3->'species'->>'name' AS evolved_pokemon,
        3 AS stage
    FROM second_stage,
    UNNEST(stage_3_json) AS evo_3
)

-- Combine everything into one flat table
SELECT * FROM (
    SELECT evolution_chain_id, base_pokemon, evolved_pokemon, stage FROM first_stage
    UNION ALL
    SELECT evolution_chain_id, base_pokemon, evolved_pokemon, stage FROM second_stage
    UNION ALL
    SELECT evolution_chain_id, base_pokemon, evolved_pokemon, stage FROM third_stage
) AS full_chain
ORDER BY evolution_chain_id, stage