{% set release_version_new = var('release_version') | int %}
{% set release_version_old = release_version_new - 1 %}

{% set columns = [
    'num', 'name', 'type', 'base_power', 'accuracy', 'pp', 'priority', 'category',
    'flags', 'is_nonstandard', 'short_description', 'description'
] %}

WITH dav_new AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_new }}/move.parquet')
),
dav_old AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_old }}/move.parquet')
)

SELECT
    dav_new.key,
    {% for col in columns %}
    CASE 
        WHEN dav_new.{{ col }} IS DISTINCT FROM dav_old.{{ col }} THEN dav_new.{{ col }}
        ELSE NULL
    END AS {{ col }}_new,
    CASE 
        WHEN dav_new.{{ col }} IS DISTINCT FROM dav_old.{{ col }} THEN dav_old.{{ col }}
        ELSE NULL
    END AS {{ col }}_old
    {%- if not loop.last %},{% endif %}
    {% endfor %}
    
FROM dav_new
LEFT JOIN dav_old ON dav_new.key = dav_old.key
WHERE
    {% for col in columns %}
    dav_new.{{ col }} IS DISTINCT FROM dav_old.{{ col }}
    {%- if not loop.last %} OR {% endif %}
    {% endfor %}
    OR dav_old.key IS NULL