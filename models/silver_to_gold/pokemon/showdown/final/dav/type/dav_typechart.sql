WITH typechart AS (
    -- Chargement des données depuis le fichier Parquet
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_typechart.parquet')
),

-- Transformation des résistances et retrait du préfixe "dmg_"
typechart_transformed AS (
    SELECT
        key AS type,
        
        {% set types = [
            'bug', 'dark', 'dragon', 'electric', 'fairy', 'fighting', 
            'fire', 'flying', 'ghost', 'grass', 'ground', 'ice', 
            'normal', 'poison', 'psychic', 'rock', 'steel', 'stellar', 'water'
        ] %}
        
        {% for t in types %}
            CASE
                WHEN dmg_{{ t }} = 3 THEN 0
                WHEN dmg_{{ t }} = 2 THEN 0.5
                WHEN dmg_{{ t }} = 1 THEN 2
                WHEN dmg_{{ t }} = 0 THEN 1
                ELSE dmg_{{ t }}
            END AS {{ t }},
        {% endfor %}
        
    FROM typechart
)

-- Sélection des colonnes sans préfixe "dmg_"
SELECT
    type,
    {% for t in types %}
        {{ t }},
    {% endfor %}
FROM typechart_transformed