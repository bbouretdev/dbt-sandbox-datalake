{{ config(materialized = 'table') }}

{% set notion_date = var('notion_date') %}

with source_data as (

  select *
  from read_json_auto(
    's3://sandbox-datalake-bronze/davlab/notion/{{ notion_date }}/sprints.json'
  )

),

exploded as (

  select
    json_extract(results_elem, '$.id')::uuid as id,
    json_extract_string(results_elem, '$.created_time') as created_time,
    json_extract_string(results_elem, '$.last_edited_time') as last_edited_time,

    json_extract_string(results_elem, '$.properties.Nom.title[0].plain_text') as name,

    json_extract_string(results_elem, '$.properties.Période du sprint.date.start')::date as period_start,
    json_extract_string(results_elem, '$.properties.Période du sprint.date.end')::date as period_end,

    json_extract(results_elem, '$.properties.Sprint actuel.formula.boolean')::boolean as current_sprint,

    -- Rollup array is left as JSON for now
    json_extract(results_elem, '$.properties.Agrégation.rollup.array') as aggregation_json,

    json_extract_string(results_elem, '$.url') as url,
    json_extract(results_elem, '$.archived')::boolean as archived,
    json_extract(results_elem, '$.in_trash')::boolean as in_trash

  from source_data,
       unnest(source_data.results) as t(results_elem)

)

select * from exploded