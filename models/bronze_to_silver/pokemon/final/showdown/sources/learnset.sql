{{ config(materialized = 'table') }}

{% set release_version = var('release_version') %}
{% set source = var('source') %}

WITH raw AS (
    SELECT 
        key AS pokemon_id,
        MAP_KEYS(learnset) AS move_ids
    FROM read_json_auto(
        's3://sandbox-datalake-bronze/pokemon/showdown/{{ source }}/{{ release_version }}/learnset.json',
        columns = {
            'key': 'VARCHAR',
            'learnset': 'MAP(VARCHAR, VARCHAR[])'
        }
    )
)

SELECT 
    pokemon_id,
    move_id.unnest AS move_id
FROM raw,
UNNEST(move_ids) AS move_id(unnest)