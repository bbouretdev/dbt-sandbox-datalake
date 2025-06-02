{% set release_version_old = var('release_version_old') %}
{% set release_version_new = var('release_version_new') %}

WITH dav_new AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_new }}/tier.parquet')
),
dav_old AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_old }}/tier.parquet')
),
diffs AS (
    SELECT
        dav_new.key,
        CASE 
            WHEN dav_new.tier IS DISTINCT FROM dav_old.tier THEN dav_new.tier 
            ELSE NULL 
        END AS tier_new,
        CASE 
            WHEN dav_new.tier IS DISTINCT FROM dav_old.tier THEN dav_old.tier 
            ELSE NULL 
        END AS tier_old
    FROM dav_new
    LEFT JOIN dav_old ON dav_new.key = dav_old.key
)

SELECT *
FROM diffs
WHERE tier_new IS NOT NULL OR tier_old IS NOT NULL