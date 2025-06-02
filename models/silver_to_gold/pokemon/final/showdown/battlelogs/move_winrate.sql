with base as (

    select
        roomid,
        player,
        winner,
        species,
        trim(lower(move)) as move
    from {{ ref('battle_team_exploded') }},
         unnest(moves) as m(move)  -- 👈 nommage explicite ici
    where moves is not null

),

move_stats as (

    select
        move,
        count(*) as total_appearances,
        sum(case when player = winner then 1 else 0 end) as total_wins,
        round(100.0 * sum(case when player = winner then 1 else 0 end) / count(*), 2) as winrate_percent
    from base
    group by move
    order by winrate_percent desc

)

select * from move_stats