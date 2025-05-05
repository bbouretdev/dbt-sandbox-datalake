WITH moves AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_move.parquet')
)

SELECT *
FROM moves
WHERE is_nonstandard IS NULL