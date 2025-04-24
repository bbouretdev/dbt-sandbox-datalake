WITH english_flavor AS (
    SELECT
        id,
        name,
        accuracy,
        power,
        pp,
        priority,
        damage_class,
        target,
        move_type,
        generation,
        flavor_entry->>'flavor_text' AS flavor_text,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY flavor_entry->'version_group'->>'name') AS row_num
    FROM {{ ref('pokeapi_move_english_flavor') }}
    WHERE flavor_entry->'language'->>'name' = 'en'
    QUALIFY row_num = 1
)

SELECT
    id,
    name,
    accuracy,
    power,
    pp,
    priority,
    damage_class,
    target,
    move_type,
    generation,
    flavor_text
FROM english_flavor
ORDER BY id