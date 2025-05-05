WITH abilities AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_ability.parquet')
)

SELECT *
FROM abilities
WHERE is_nonstandard IS NULL