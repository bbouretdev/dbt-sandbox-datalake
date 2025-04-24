{{ config(materialized = 'table') }}

{% set columns = [
    'name', 'num', 'gen', 'is_nonstandard', 'is_berry', 'is_gem', 'is_pokeball', 
    'description', 'short_description', 'onDrive', 'megaStone', 'megaEvolves', 
    'zMove', 'zMoveType', 'zMoveFrom', 'itemUser'
] %}

WITH dav AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_item.parquet')
),
smogon AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/smogon/showdown_smogon_item.parquet')
)

SELECT
    dav.key,
    {% for col in columns %}
    -- Comparaison des colonnes classiques : 'new' et 'old'
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