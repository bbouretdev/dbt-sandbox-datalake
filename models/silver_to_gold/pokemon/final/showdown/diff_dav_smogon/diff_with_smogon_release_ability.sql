{% set release_version = var('release_version') %}

{% set columns = [
    "rating", "num", "is_nonstandard", "short_description", "description"
] %}

WITH dav AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version }}/ability.parquet')
),
smogon AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/smogon/ability.parquet')
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