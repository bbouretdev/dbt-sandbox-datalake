{{ config(materialized = 'ephemeral') }}

SELECT *
FROM read_json_auto('s3://sandbox-datalake-bronze/pokemon/pokeapi/evolution-chain/*.json')