{% set release_version_old = var('release_version_old') %}
{% set release_version_new = var('release_version_new') %}

{% set columns = [
  "num", "name", "type_1", "type_2",
  "gender_ratio_m", "gender_ratio_f",
  "hp", "atk", "def", "spa", "spd", "spe",
  "ability_0", "ability_1", "hidden_ability",
  "height_m", "weight_kg", "color"
] %}

WITH dav_new AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_new }}/pokemon.parquet')
),
dav_old AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_old }}/pokemon.parquet')
),
diffs AS (
    SELECT
        dav_new.key
        {% for col in columns %}
            , CASE WHEN dav_new.{{ col }} IS DISTINCT FROM dav_old.{{ col }} THEN dav_new.{{ col }} END AS {{ col }}_new
            , CASE WHEN dav_new.{{ col }} IS DISTINCT FROM dav_old.{{ col }} THEN dav_old.{{ col }} END AS {{ col }}_old
        {% endfor %}
    FROM dav_new
    LEFT JOIN dav_old ON dav_new.key = dav_old.key
)

SELECT *
FROM diffs
WHERE
    {% for col in columns %}
        {{ col }}_new IS NOT NULL{% if not loop.last %} OR{% endif %}
    {% endfor %}