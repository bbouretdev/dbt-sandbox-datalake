{{ config(materialized = 'table') }}

{% set release_version = var('release_version') %}
{% set source = var('source') %}

SELECT
    key AS key,
    name AS name,
    "desc" AS description,
    shortDesc AS short_description,
    num AS num,
    isNonstandard AS is_nonstandard,
    gen AS gen,
    isBerry AS is_berry,
    isGem AS is_gem,
    isPokeball AS is_pokeball,
    onDrive AS on_drive,
    megaStone AS mega_stone,
    megaEvolves AS mega_evolves,
    zMove AS z_move,
    zMoveType AS z_move_type,
    zMoveFrom AS z_move_from,
    itemUser AS item_user,
    boosts AS boosts,
    flags AS flags

FROM read_json_auto(
    's3://sandbox-datalake-bronze/pokemon/showdown/{{ source }}/{{ release_version }}/item.json',
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
        'itemUser': 'VARCHAR',
        'boosts': 'MAP(VARCHAR, INTEGER)',
        'flags': 'MAP(VARCHAR, BOOLEAN)'
    }
)