{{ config(materialized = 'ephemeral') }}

SELECT *
FROM read_json_auto(
    's3://sandbox-datalake-bronze/pokemon/showdown/smogon/tier/*.json',
    columns = {
        'key': 'VARCHAR',
        'tier': 'VARCHAR'
    }
)