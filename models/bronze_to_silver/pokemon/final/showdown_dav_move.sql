-- depends_on: {{ ref('showdown_dav_move_raw') }}

{{ config(materialized = 'table') }}

WITH flattened AS (
    SELECT
        key,  -- Clé unique du move
        num::int AS num,  -- Numéro du move
        name,  -- Nom du move
        COALESCE(type, NULL) AS type,  -- Type du move
        COALESCE(basePower, NULL) AS basepower,  -- Puissance du move
        COALESCE(accuracy, NULL) AS accuracy,  -- Exactitude du move
        COALESCE(pp, NULL) AS pp,  -- Points de pouvoir
        COALESCE(priority, NULL) AS priority,  -- Priorité
        COALESCE(category, NULL) AS category,  -- Catégorie du move
        COALESCE(flags, NULL) AS flags,  -- Flags associés au move
        COALESCE(isNonstandard, NULL) AS is_nonstandard,  -- Is non-standard
        COALESCE(shortDesc, NULL) AS short_description,  -- Description courte
        COALESCE("desc", NULL) AS description,  -- Description longue
        COALESCE(onResidualOrder, NULL) AS on_residual_order,  -- Résidus d'ordre
        COALESCE(onDamagingHitOrder, NULL) AS on_damaging_hit_order,  -- Résidu sur coup dommageable
        COALESCE(condition, NULL) AS condition,  -- Condition pour utiliser le move
        COALESCE(onResidualSubOrder, NULL) AS on_residual_sub_order,  -- Sous-ordre de résidus
        COALESCE(onHitPriority, NULL) AS on_hit_priority  -- Priorité sur hit
    FROM {{ ref('showdown_dav_move_raw') }}
)

SELECT * FROM flattened