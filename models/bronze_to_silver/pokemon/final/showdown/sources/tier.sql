{{ config(materialized = 'table') }}

{% set release_version = var('release_version') %}
{% set source = var('source') %}

SELECT
    key as key,
    tier as tier,
    doublesTier as doubles_tier,
    isNonstandard as is_nonstandard
FROM read_json_auto(
    's3://sandbox-datalake-bronze/pokemon/showdown/{{ source }}/{{ release_version }}/tier.json',
    columns = {
        'key': 'VARCHAR',
        'tier': 'VARCHAR',
        'doublesTier': 'VARCHAR',
        'isNonstandard': 'VARCHAR'
    }
)