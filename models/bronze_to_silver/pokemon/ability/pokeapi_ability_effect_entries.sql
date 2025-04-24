{{ config(materialized = 'ephemeral') }}

WITH raw AS (
    SELECT * FROM {{ ref('pokeapi_ability_raw') }}
),
english_effects AS (
    SELECT
        id AS id,
        name AS name,
        UNNEST(effect_entries) AS effect_entry
    FROM raw
    WHERE array_length(effect_entries) > 0
),
filtered AS (
    SELECT
        id,
        name,
        effect_entry->>'effect' AS effect,
        effect_entry->>'short_effect' AS short_effect
    FROM english_effects
    WHERE effect_entry->'language'->>'name' = 'en'
)

SELECT * FROM filtered
ORDER BY id