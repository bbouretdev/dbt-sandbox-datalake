{% set release_version = var('release_version') %}
{% set source = var('source', 'dav') %}

{% if source == 'dav' %}
    {% set input_path = 's3://sandbox-datalake-bronze/pokemon/showdown/dav/' ~ release_version ~ '/typechart.json' %}
{% elif source == 'smogon' %}
    {% set input_path = 's3://sandbox-datalake-bronze/pokemon/showdown/smogon/typechart.json' %}
{% endif %}

{% set damage_types = [
    "Bug", "Dark", "Dragon", "Electric", "Fairy", "Fighting", "Fire", "Flying", "Ghost",
    "Grass", "Ground", "Ice", "Normal", "Poison", "Psychic", "Rock", "Steel", "Stellar", "Water"
] %}

{% set status_types = [
    "prankster", "par", "brn", "trapped", "powder", "sandstorm", "hail", "frz", "psn", "tox"
] %}

{% set iv_stats = ["atk", "def", "spd", "spa", "spe", "hp"] %}
{% set dv_stats = ["atk", "def"] %}

WITH raw_json AS (
    SELECT *
    FROM read_json_auto(
        '{{ input_path }}',
        columns = {
            'key': 'VARCHAR',
            'damageTaken': 'STRUCT(Bug BIGINT, Dark BIGINT, Dragon BIGINT, Electric BIGINT, Fairy BIGINT, Fighting BIGINT, Fire BIGINT, Flying BIGINT, Ghost BIGINT, Grass BIGINT, Ground BIGINT, Ice BIGINT, Normal BIGINT, Poison BIGINT, Psychic BIGINT, Rock BIGINT, Steel BIGINT, Stellar BIGINT, Water BIGINT, prankster BIGINT, par BIGINT, brn BIGINT, trapped BIGINT, powder BIGINT, sandstorm BIGINT, hail BIGINT, frz BIGINT, psn BIGINT, tox BIGINT)',
            'HPivs': 'STRUCT(atk BIGINT, def BIGINT, spd BIGINT, spa BIGINT, spe BIGINT, hp BIGINT)',
            'HPdvs': 'STRUCT(atk BIGINT, def BIGINT)'
        }
    )
),

flattened AS (
    SELECT
        key AS key
        {% for t in damage_types %}
            , damageTaken.{{ t }} AS dmg_{{ t | lower }}
        {% endfor %}
        {% for t in status_types %}
            , damageTaken.{{ t }} AS dmg_{{ t }}
        {% endfor %}
        {% for stat in iv_stats %}
            , HPivs.{{ stat }} AS iv_{{ stat }}
        {% endfor %}
        {% for stat in dv_stats %}
            , HPdvs.{{ stat }} AS dv_{{ stat }}
        {% endfor %}
    FROM raw_json
)

SELECT * FROM flattened