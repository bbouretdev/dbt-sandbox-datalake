{{ config(materialized = 'table') }}

with tickets as (
  select *
  from {{ ref('tickets') }}
),

sprints as (
  select
    sprint_id,
    name,
    period_start,
    period_end,
    case
      when name is not null
      then substring(substring(name, 1, position(' - ' in name) - 1), position('Sprint ' in name) + 7)
      else null
    end as sprint_number
  from {{ ref('sprints') }}
)

select
  t.*,
  s.sprint_number,
  s.name as sprint_name,
  s.period_start as sprint_start,
  s.period_end as sprint_end
from tickets t
left join sprints s on t.sprint_id = s.sprint_id