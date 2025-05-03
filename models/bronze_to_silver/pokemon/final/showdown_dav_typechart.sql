-- depends_on: {{ ref('showdown_dav_typechart_raw') }}

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
    FROM {{ ref('showdown_dav_typechart_raw') }}
),

flattened AS (
    SELECT
        key
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