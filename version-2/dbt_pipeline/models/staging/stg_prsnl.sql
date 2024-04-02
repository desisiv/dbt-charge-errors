with source as (

    {#-
    Normally we would select from the table here,
    but we are using Trino to load our data in this project
    #}
    select * from {{ var('mill_catalog') }}.{{ var('mill_schema') }}.prsnl

),

renamed as (

    select
        person_id,
        name_full_formatted,
        position_cd,
        active_ind,
        cast(beg_effective_dt_tm as timestamp(6)) as beg_effective_dt_tm,
        cast(end_effective_dt_tm as timestamp(6)) as end_effective_dt_tm

    from source

)

select * from renamed