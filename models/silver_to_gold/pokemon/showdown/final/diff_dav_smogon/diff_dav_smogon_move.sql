{{ config(materialized = 'table') }}

{% set columns = [
    'num', 'name', 'type', 'basepower', 'accuracy', 'pp', 'priority', 'category',
    'flags', 'is_nonstandard', 'short_description', 'description',
    'on_residual_order', 'on_damaging_hit_order', 'condition',
    'on_residual_sub_order', 'on_hit_priority'
] %}

WITH dav AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_move.parquet')
),
smogon AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/smogon/showdown_smogon_move.parquet')
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