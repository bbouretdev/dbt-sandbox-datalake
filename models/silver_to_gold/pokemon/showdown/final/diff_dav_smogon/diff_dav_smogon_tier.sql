{{ config(materialized = 'table') }}

WITH dav AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_tier.parquet')
),
smogon AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/smogon/showdown_smogon_tier.parquet')
),
diffs AS (
    SELECT
        dav.key,
        CASE 
            WHEN dav.tier IS DISTINCT FROM smogon.tier THEN dav.tier 
            ELSE NULL 
        END AS tier_new,
        CASE 
            WHEN dav.tier IS DISTINCT FROM smogon.tier THEN smogon.tier 
            ELSE NULL 
        END AS tier_old
    FROM dav
    LEFT JOIN smogon ON dav.key = smogon.key
)

SELECT *
FROM diffs
WHERE tier_new IS NOT NULL OR tier_old IS NOT NULL