WITH typechart AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_typechart.parquet')
)

SELECT *
FROM typechart