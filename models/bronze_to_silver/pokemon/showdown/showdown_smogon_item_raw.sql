{{ config(materialized = 'ephemeral') }}

SELECT *
FROM read_json_auto(
    's3://sandbox-datalake-bronze/pokemon/showdown/smogon/item/*.json',
    columns = {
        'key': 'VARCHAR',
        'name': 'VARCHAR',
        'desc': 'VARCHAR',
        'shortDesc': 'VARCHAR',
        'num': 'INTEGER',
        'isNonstandard': 'VARCHAR',
        'gen': 'INTEGER',
        'isBerry': 'BOOLEAN',
        'isGem': 'BOOLEAN',
        'isPokeball': 'BOOLEAN',
        'onDrive': 'VARCHAR',
        'megaStone': 'VARCHAR',
        'megaEvolves': 'VARCHAR',
        'zMove': 'VARCHAR',
        'zMoveType': 'VARCHAR',
        'zMoveFrom': 'VARCHAR',
        'itemUser': 'VARCHAR', -- Remplace LIST(VARCHAR) par VARCHAR
        'boosts': 'MAP(VARCHAR, INTEGER)', -- MAP reste une bonne solution pour des paires clé-valeur
        'flags': 'MAP(VARCHAR, BOOLEAN)'
    }
)