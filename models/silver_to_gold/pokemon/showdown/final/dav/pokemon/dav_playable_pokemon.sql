WITH pokemons AS (
    SELECT *
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_pokemon.parquet')
),
playable AS (
    SELECT key, tier
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/showdown_dav_tier.parquet')
    WHERE tier IN ('OU', 'LC')
)

SELECT poke.*
FROM pokemons poke
INNER JOIN playable pl ON pl.key = poke.key