SELECT *
FROM {{ ref('pokeapi_item_english_flavor') }}
ORDER BY id