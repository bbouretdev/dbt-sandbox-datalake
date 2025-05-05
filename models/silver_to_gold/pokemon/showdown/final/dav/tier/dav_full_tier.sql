WITH tiers AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_tier.parquet')
)

SELECT *
FROM tiers