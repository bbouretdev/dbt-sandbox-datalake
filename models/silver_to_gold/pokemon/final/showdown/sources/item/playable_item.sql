WITH items AS (
    SELECT *
    FROM {{ ref('item') }}
)

SELECT *
FROM items
WHERE is_nonstandard IS NULL