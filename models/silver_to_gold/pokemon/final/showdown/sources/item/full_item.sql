WITH items AS (
    SELECT *
    FROM {{ ref('item') }}
)

SELECT *
FROM items