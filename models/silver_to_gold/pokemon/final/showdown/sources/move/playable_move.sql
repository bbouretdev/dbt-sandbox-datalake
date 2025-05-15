WITH moves AS (
    SELECT *
    FROM {{ ref('move') }}
)

SELECT *
FROM moves
WHERE is_nonstandard IS NULL