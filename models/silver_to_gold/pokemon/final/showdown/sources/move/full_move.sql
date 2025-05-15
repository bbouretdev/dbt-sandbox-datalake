WITH moves AS (
    SELECT *
    FROM {{ ref('move') }}
)

SELECT *
FROM moves