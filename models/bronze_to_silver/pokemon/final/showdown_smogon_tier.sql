-- depends_on: {{ ref('showdown_smogon_tier_raw') }}

{{ config(materialized = 'table') }}

WITH flattened AS (
    SELECT
        key,
        tier
    FROM {{ ref('showdown_smogon_tier_raw') }}
)

SELECT * FROM flattened