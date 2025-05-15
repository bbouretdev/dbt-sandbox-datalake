{{ config(materialized = 'table') }}

{% set release_version = var('release_version') %}

{% set columns = [
  "num", "name", "type_1", "type_2",
  "gender_ratio_m", "gender_ratio_f",
  "hp", "atk", "def", "spa", "spd", "spe",
  "ability_0", "ability_1", "hidden_ability",
  "height_m", "weight_kg", "color"
] %}

WITH dav AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version }}/pokemon.parquet')
),
smogon AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/smogon/{{ release_version }}/pokemon.parquet')
),
diffs AS (
    SELECT
        dav.key
        {% for col in columns %}
            , CASE WHEN dav.{{ col }} IS DISTINCT FROM smogon.{{ col }} THEN dav.{{ col }} END AS {{ col }}_new
            , CASE WHEN dav.{{ col }} IS DISTINCT FROM smogon.{{ col }} THEN smogon.{{ col }} END AS {{ col }}_old
        {% endfor %}
    FROM dav
    LEFT JOIN smogon ON dav.key = smogon.key
)

SELECT *
FROM diffs
WHERE
    {% for col in columns %}
        {{ col }}_new IS NOT NULL{% if not loop.last %} OR{% endif %}
    {% endfor %}