{{ config(materialized = 'table') }}

{% set release_version = var('release_version') %}

{% set columns = [
    'num', 'name', 'type', 'base_power', 'accuracy', 'pp', 'priority', 'category',
    'flags', 'is_nonstandard', 'short_description', 'description'
] %}

WITH dav AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version }}/move.parquet')
),
smogon AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/smogon/move.parquet')
)

SELECT
    dav.key,
    {% for col in columns %}
    CASE 
        WHEN dav.{{ col }} IS DISTINCT FROM smogon.{{ col }} THEN dav.{{ col }}
        ELSE NULL
    END AS {{ col }}_new,
    CASE 
        WHEN dav.{{ col }} IS DISTINCT FROM smogon.{{ col }} THEN smogon.{{ col }}
        ELSE NULL
    END AS {{ col }}_old
    {%- if not loop.last %},{% endif %}
    {% endfor %}
    
FROM dav
LEFT JOIN smogon ON dav.key = smogon.key
WHERE
    {% for col in columns %}
    dav.{{ col }} IS DISTINCT FROM smogon.{{ col }}
    {%- if not loop.last %} OR {% endif %}
    {% endfor %}
    OR smogon.key IS NULL