{{ config(materialized = 'table') }}

SELECT * FROM {{ ref('showdown_dav_pokemon_flatten') }}