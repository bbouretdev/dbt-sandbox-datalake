WITH abilities AS (
    SELECT *
    FROM {{ ref('ability') }}
)

SELECT *
FROM abilities