{{ config(materialized = 'table') }}

{% set notion_date = var('notion_date') %}

with source_data as (

  select *
  from read_json_auto(
    's3://sandbox-datalake-bronze/davlab/notion/{{ notion_date }}/tickets.json'
  )

),

exploded as (

  select
    json_extract(results_elem, '$.id')::uuid as id,
    json_extract_string(results_elem, '$.created_time') as created_time,
    json_extract_string(results_elem, '$.last_edited_time') as last_edited_time,

    json_extract_string(results_elem, '$.properties.Nom.title[0].plain_text') as name,

    -- Simple fields
    json_extract_string(results_elem, '$.properties.État.status.name') as status,
    json_extract_string(results_elem, '$.properties.Chemin.relation[0].id')::uuid as path_id,
    json_extract_string(results_elem, '$.properties.Sprint.relation[0].id')::uuid as sprint_id,
    json_extract_string(results_elem, '$.properties.Projet.relation[0].id')::uuid as project_id,

    -- Calculated / rollup fields
    json_extract(results_elem, '$.properties.Sprint actuel.rollup.array[0].formula.boolean')::boolean as current_sprint,
    json_extract(results_elem, '$.properties.Agrégation.rollup.array[0].number')::int as aggregation,
    json_extract(results_elem, '$.properties.Nb Tickets Done.formula.number')::int as nb_tickets_done,
    json_extract(results_elem, '$.properties.Nb Tickets Total.formula.number')::int as nb_tickets_total,

    -- ID and Points
    json_extract_string(results_elem, '$.properties.ID.unique_id.prefix') as ticket_prefix,
    json_extract(results_elem, '$.properties.ID.unique_id.number')::int as ticket_number,
    json_extract_string(results_elem, '$.properties.Points.relation[0].id')::uuid as point_id,

    -- Assignment, tags, criticality
    json_extract_string(results_elem, '$.properties.Attribué à.people[0].name') as assignee,
    json_extract_string(results_elem, '$.properties.Tags.select.name') as tag,
    json_extract_string(results_elem, '$.properties.Criticité.select.name') as criticality,

    -- Last modifications
    json_extract_string(results_elem, '$.properties.Dernière modification.last_edited_time') as last_modified_at,
    json_extract_string(results_elem, '$.properties.Date de création.created_time') as created_at,
    json_extract_string(results_elem, '$.properties.Dernière modification par.last_edited_by.name') as last_modified_by,

    -- Misc fields
    json_extract_string(results_elem, '$.url') as url,
    json_extract(results_elem, '$.archived')::boolean as archived,
    json_extract(results_elem, '$.in_trash')::boolean as in_trash

  from source_data,
       unnest(source_data.results) as t(results_elem)

)

select * from exploded