{{ config(materialized = 'table') }}

SELECT 
    try_cast(key AS VARCHAR) AS pokemon_id,
    unnest as move_id
FROM 
    read_json_auto('s3://sandbox-datalake-bronze/pokemon/showdown/smogon/learnset/*.json'),
    UNNEST(MAP_KEYS(try_cast(learnset AS MAP(VARCHAR, VARCHAR)))) AS unnest