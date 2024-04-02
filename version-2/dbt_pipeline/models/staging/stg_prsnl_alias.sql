with source as (

    {#-
    Normally we would select from the table here,
    but we are using Trino to load our data in this project
    #}
    select * from {{ var('mill_catalog') }}.{{ var('mill_schema') }}.prsnl_alias

),

renamed as (

    select
        prsnl_alias_id,
        person_id,
        prsnl_alias_type_cd,
        alias,
        active_ind,
        cast(beg_effective_dt_tm as timestamp(6)) as beg_effective_dt_tm,
        cast(end_effective_dt_tm as timestamp(6)) as end_effective_dt_tm

    from source

)

select * from renamed