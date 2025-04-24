{{ config(materialized = 'ephemeral') }}

WITH raw AS (
    SELECT * FROM {{ ref('pokeapi_item_raw') }}
),
flavors AS (
    SELECT
        id AS id,
        name AS name,
        UNNEST(flavor_text_entries) AS flavor_entry
    FROM raw
),
english_flavor AS (
    SELECT
        id,
        name,
        flavor_entry->>'text' AS flavor_text,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY flavor_entry->'version_group'->>'name') AS row_num
    FROM flavors
    WHERE flavor_entry->'language'->>'name' = 'en'
    QUALIFY row_num = 1
)

SELECT *
FROM english_flavor