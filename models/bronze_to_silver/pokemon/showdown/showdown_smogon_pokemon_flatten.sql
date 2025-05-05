{{ config(materialized = 'ephemeral') }}

WITH flattened as (
    select
        key as key,
        num::int as num,
        name as name,
        types[1] as type_1,
        types[2] as type_2,
        genderRatio.M as gender_ratio_m,
        genderRatio.F as gender_ratio_f,
        baseStats.hp as hp,
        baseStats.atk as atk,
        baseStats.def as def,
        baseStats.spa as spa,
        baseStats.spd as spd,
        baseStats.spe as spe,
        abilities['0'] as ability_0,
        abilities['1'] as ability_1,
        abilities['H'] as hidden_ability,
        heightm::float as height_m,
        weightkg::float as weight_kg,
        color,
        tier
    FROM {{ ref('showdown_smogon_pokemon_raw') }}
)

select * from flattened