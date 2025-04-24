{{ config(materialized = 'table') }}

WITH flattened AS (
    SELECT
        COALESCE(key, NULL) AS key,
        COALESCE(name, NULL) AS name,
        COALESCE(rating::float, NULL) AS rating,
        COALESCE(num::int, NULL) AS num,
        COALESCE(isNonstandard, NULL) AS is_nonstandard,
        COALESCE(shortDesc, NULL) AS short_description,
        COALESCE("desc", NULL) AS description,
        COALESCE(flags, NULL) AS flags
    FROM {{ ref('showdown_dav_ability_raw') }}
)

SELECT * FROM flattened