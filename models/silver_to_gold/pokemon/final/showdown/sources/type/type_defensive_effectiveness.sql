WITH typechart AS (
    SELECT *
    FROM {{ ref('typechart') }}
),
unpivoted AS (
    SELECT key, 'bug' AS attack_type, dmg_bug AS effectiveness FROM typechart
    UNION ALL SELECT key, 'dark', dmg_dark FROM typechart
    UNION ALL SELECT key, 'dragon', dmg_dragon FROM typechart
    UNION ALL SELECT key, 'electric', dmg_electric FROM typechart
    UNION ALL SELECT key, 'fairy', dmg_fairy FROM typechart
    UNION ALL SELECT key, 'fighting', dmg_fighting FROM typechart
    UNION ALL SELECT key, 'fire', dmg_fire FROM typechart
    UNION ALL SELECT key, 'flying', dmg_flying FROM typechart
    UNION ALL SELECT key, 'ghost', dmg_ghost FROM typechart
    UNION ALL SELECT key, 'grass', dmg_grass FROM typechart
    UNION ALL SELECT key, 'ground', dmg_ground FROM typechart
    UNION ALL SELECT key, 'ice', dmg_ice FROM typechart
    UNION ALL SELECT key, 'normal', dmg_normal FROM typechart
    UNION ALL SELECT key, 'poison', dmg_poison FROM typechart
    UNION ALL SELECT key, 'psychic', dmg_psychic FROM typechart
    UNION ALL SELECT key, 'rock', dmg_rock FROM typechart
    UNION ALL SELECT key, 'steel', dmg_steel FROM typechart
    UNION ALL SELECT key, 'stellar', dmg_stellar FROM typechart
    UNION ALL SELECT key, 'water', dmg_water FROM typechart
),
immune AS (
    SELECT key, attack_type FROM unpivoted WHERE effectiveness = 3
),
resistant AS (
    SELECT key, attack_type FROM unpivoted WHERE effectiveness = 2
),
weak AS (
    SELECT key, attack_type FROM unpivoted WHERE effectiveness = 1
),
neutral AS (
    SELECT key, attack_type FROM unpivoted WHERE effectiveness = 0
),
grouped AS (
    SELECT
        key AS type_def,
        (SELECT json_group_array(attack_type) FROM immune WHERE immune.key = u.key) AS immune_to,
        (SELECT json_group_array(attack_type) FROM resistant WHERE resistant.key = u.key) AS resistant_to,
        (SELECT json_group_array(attack_type) FROM weak WHERE weak.key = u.key) AS weak_to,
        (SELECT json_group_array(attack_type) FROM neutral WHERE neutral.key = u.key) AS neutral_to
    FROM (SELECT DISTINCT key FROM unpivoted) u
)
SELECT *
FROM grouped
ORDER BY type_def