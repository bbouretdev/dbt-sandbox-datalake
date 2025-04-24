{{ config(materialized = 'table') }}

SELECT * FROM {{ ref('showdown_smogon_pokemon_flatten') }}