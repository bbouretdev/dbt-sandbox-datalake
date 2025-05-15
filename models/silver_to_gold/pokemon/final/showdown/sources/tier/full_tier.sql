WITH tiers AS (
    SELECT *
    FROM {{ ref('tier') }}
)

SELECT *
FROM tiers