{{ config(materialized = 'ephemeral') }}

SELECT *
FROM read_json_auto(
    's3://sandbox-datalake-bronze/pokemon/showdown/dav/ability/*.json',
    columns = {
        'key': 'VARCHAR',
        'name': 'VARCHAR',
        'rating': 'FLOAT',
        'num': 'INTEGER',
        'isNonstandard': 'VARCHAR',
        'shortDesc': 'VARCHAR',
        'desc': 'VARCHAR',
        'flags': 'MAP(VARCHAR, BOOLEAN)'  -- ou 'JSON' si tu veux garder la structure
    }
)