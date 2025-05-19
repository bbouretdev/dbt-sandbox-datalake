{{ config(materialized = 'table') }}

{% set release_version = var('release_version') %}
{% set source = var('source', 'dav') %}

{% if source == 'dav' %}
    {% set input_path = 's3://sandbox-datalake-bronze/pokemon/showdown/dav/' ~ release_version ~ '/tier.json' %}
{% elif source == 'smogon' %}
    {% set input_path = 's3://sandbox-datalake-bronze/pokemon/showdown/smogon/tier.json' %}
{% endif %}

SELECT
    key as key,
    tier as tier,
    doublesTier as doubles_tier,
    isNonstandard as is_nonstandard
FROM read_json_auto(
    '{{ input_path }}',
    columns = {
        'key': 'VARCHAR',
        'tier': 'VARCHAR',
        'doublesTier': 'VARCHAR',
        'isNonstandard': 'VARCHAR'
    }
)