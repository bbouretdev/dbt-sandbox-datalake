{% set release_version_new = var('release_version') | int %}
{% set release_version_old = release_version_new - 1 %}

WITH dav_new AS (
    SELECT pokemon_id, move_id
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_new }}/learnset.parquet')
),
dav_old AS (
    SELECT pokemon_id, move_id
    FROM read_parquet('s3://sandbox-datalake-silver/pokemon/showdown/dav/{{ release_version_old }}/learnset.parquet')
),

-- Calcul des moves ajoutés (présents dans dav_new mais pas dans dav_old)
added_moves AS (
    SELECT pokemon_id, move_id
    FROM dav_new
    EXCEPT
    SELECT pokemon_id, move_id
    FROM dav_old
),

-- Calcul des moves supprimés (présents dans dav_old mais pas dans dav_new)
removed_moves AS (
    SELECT pokemon_id, move_id
    FROM dav_old
    EXCEPT
    SELECT pokemon_id, move_id
    FROM dav_new
),

-- Groupement des moves ajoutés et supprimés par pokemon_id
grouped AS (
    SELECT
        COALESCE(a.pokemon_id, r.pokemon_id) AS pokemon_id,
        list(DISTINCT a.move_id) FILTER (WHERE a.move_id IS NOT NULL) AS added_moves,
        list(DISTINCT r.move_id) FILTER (WHERE r.move_id IS NOT NULL) AS removed_moves
    FROM added_moves a
    FULL OUTER JOIN removed_moves r
        ON a.pokemon_id = r.pokemon_id
    GROUP BY 1
),

-- Filtrage pour ne garder que les pokemon_id avec des moves ajoutés ou supprimés
filtered AS (
    SELECT *
    FROM grouped
    WHERE array_length(added_moves) > 0 OR array_length(removed_moves) > 0
)

SELECT
    pokemon_id,
    to_json(added_moves) AS added_moves,
    to_json(removed_moves) AS removed_moves
FROM filtered