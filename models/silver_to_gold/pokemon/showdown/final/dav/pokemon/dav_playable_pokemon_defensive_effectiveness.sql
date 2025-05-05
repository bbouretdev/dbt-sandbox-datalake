WITH typechart AS (
    SELECT
        type,
        bug, dark, dragon, electric, fairy, fighting, fire, flying, ghost, grass, ground,
        ice, normal, poison, psychic, rock, steel, stellar, water
    FROM {{ ref('dav_typechart') }}
),

pokemon AS (
    SELECT 
        key AS pokemon_name, 
        type_1, 
        type_2
    FROM {{ ref('dav_playable_pokemon') }}
),

resistance AS (
    SELECT
        p.pokemon_name,
        p.type_1,
        p.type_2,
        {% set types = ['bug', 'dark', 'dragon', 'electric', 'fairy', 'fighting', 
                        'fire', 'flying', 'ghost', 'grass', 'ground', 'ice', 
                        'normal', 'poison', 'psychic', 'rock', 'steel', 'stellar', 'water'] %}
        {% for t in types %}
            COALESCE(tc1.{{ t }}, 1) * COALESCE(tc2.{{ t }}, 1) AS {{ t }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM pokemon p
    LEFT JOIN typechart tc1 ON lower(p.type_1) = lower(tc1.type)
    LEFT JOIN typechart tc2 ON lower(p.type_2) = lower(tc2.type)
)

SELECT * 
FROM resistance