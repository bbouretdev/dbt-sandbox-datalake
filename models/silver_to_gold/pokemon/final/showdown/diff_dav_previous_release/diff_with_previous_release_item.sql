{% set release_version_new = var('release_version') | int %}
{% set release_version_old = release_version_new - 1 %}

{% set columns = [
    'name', 'num', 'gen', 'is_nonstandard', 'is_berry', 'is_gem', 'is_pokeball', 
    'description', 'short_description', 'on_drive', 'mega_stone', 'mega_evolves', 
    'z_move', 'z_move_type', 'z_move_from', 'item_user'
] %}

WITH dav_new AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_new }}/item.parquet')
),
dav_old AS (
    SELECT * FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_old }}/item.parquet')
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