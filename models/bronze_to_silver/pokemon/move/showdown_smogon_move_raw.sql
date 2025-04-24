{{ config(materialized = 'ephemeral') }}

SELECT *
FROM read_json_auto(
    's3://sandbox-datalake-bronze/pokemon/showdown/smogon/move/*.json',
    columns = {
        'key': 'VARCHAR',
        'num': 'INTEGER',
        'name': 'VARCHAR',
        'type': 'VARCHAR',
        'basePower': 'INTEGER',
        'accuracy': 'INTEGER',
        'pp': 'INTEGER',
        'priority': 'INTEGER',
        'category': 'VARCHAR',
        'flags': 'MAP(VARCHAR, BOOLEAN)',
        'isNonstandard': 'VARCHAR',
        'shortDesc': 'VARCHAR',
        'desc': 'VARCHAR',
        'onResidualOrder': 'INTEGER',
        'onDamagingHitOrder': 'INTEGER',
        'onResidualSubOrder': 'INTEGER',
        'onHitPriority': 'INTEGER',
        'condition': 'VARCHAR'
    }
)