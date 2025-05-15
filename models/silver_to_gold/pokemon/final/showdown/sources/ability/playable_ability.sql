WITH abilities AS (
    SELECT *
    FROM {{ ref('ability') }}
)

SELECT *
FROM abilities
WHERE is_nonstandard IS NULL