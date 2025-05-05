WITH items AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_item.parquet')
)

SELECT *
FROM items
WHERE is_nonstandard IS NULL