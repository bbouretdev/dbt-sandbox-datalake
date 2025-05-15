WITH pokemons AS (
    SELECT *
    FROM {{ ref('pokemon') }}
)

SELECT *
FROM pokemons