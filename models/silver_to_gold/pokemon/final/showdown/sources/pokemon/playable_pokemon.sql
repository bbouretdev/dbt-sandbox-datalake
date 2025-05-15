WITH pokemons AS (
    SELECT *
    FROM {{ ref('pokemon') }}
),
playable AS (
    SELECT key, tier
    FROM {{ ref('tier') }}
    WHERE tier IN ('OU', 'LC')
)

SELECT poke.*
FROM pokemons poke
INNER JOIN playable pl ON pl.key = poke.key