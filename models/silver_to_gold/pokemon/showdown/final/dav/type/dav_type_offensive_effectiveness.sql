WITH typechart AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_typechart.parquet')
),
unpivoted AS (
    SELECT key AS type_def, 'bug' AS attack_type, dmg_bug AS effectiveness FROM typechart
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
strong AS (
    SELECT attack_type, type_def FROM unpivoted WHERE effectiveness = 1
),
weak AS (
    SELECT attack_type, type_def FROM unpivoted WHERE effectiveness = 2
),
ineffective AS (
    SELECT attack_type, type_def FROM unpivoted WHERE effectiveness = 3
),
neutral AS (
    SELECT attack_type, type_def FROM unpivoted WHERE effectiveness = 0
),
grouped AS (
    SELECT
        u.attack_type,
        (SELECT json_group_array(type_def) FROM strong WHERE strong.attack_type = u.attack_type) AS strong_against,
        (SELECT json_group_array(type_def) FROM weak WHERE weak.attack_type = u.attack_type) AS weak_against,
        (SELECT json_group_array(type_def) FROM ineffective WHERE ineffective.attack_type = u.attack_type) AS ineffective_against,
        (SELECT json_group_array(type_def) FROM neutral WHERE neutral.attack_type = u.attack_type) AS neutral_against
    FROM (SELECT DISTINCT attack_type FROM unpivoted) u
)
SELECT *
FROM grouped
ORDER BY attack_type