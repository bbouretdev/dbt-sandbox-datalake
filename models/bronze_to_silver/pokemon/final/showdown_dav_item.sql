-- depends_on: {{ ref('showdown_dav_item_raw') }}

{{ config(materialized = 'table') }}

WITH flattened AS (
    SELECT
        key,
        name,
        num,
        gen,
        isNonstandard AS is_nonstandard,
        isBerry AS is_berry,
        isGem AS is_gem,
        isPokeball AS is_pokeball,
        "desc" AS description,
        shortDesc AS short_description,
        onDrive,
        megaStone,
        megaEvolves,
        zMove,
        zMoveType,
        zMoveFrom,
        itemUser,  -- On conserve l'élément comme VARCHAR
        boosts,
        flags
    FROM {{ ref('showdown_dav_item_raw') }}
)

SELECT * FROM flattened