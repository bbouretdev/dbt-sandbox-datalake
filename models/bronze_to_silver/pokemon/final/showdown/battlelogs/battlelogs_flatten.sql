{% set date = var('date') %}

with raw_json as (

    -- Lecture du fichier JSON plat depuis S3
    select 
        CAST('{{ date }}' AS DATE) as log_date,
        format,
        match.*
    from read_json_auto('s3://sandbox-datalake-bronze/pokemon/showdown/battlelogs/{{ date }}.json')

),

filtered as (

    -- On ne garde que les nouvelles données en incrémental
    select * from raw_json

    {% if is_incremental() %}
    where log_date > (select max(log_date) from {{ this }})
    {% endif %}

)

select * from filtered