with source as (

    {#-
    Normally we would select from the table here,
    but we are using Trino to load our data in this project
    #}
    select * from {{ var('mill_catalog') }}.{{ var('mill_schema') }}.code_value

),

renamed as (

    select
        code_value,
        code_set,
        cdf_meaning,
        display,
        cki,
        active_ind,
        cast(begin_effective_dt_tm as timestamp(6)) as beg_effective_dt_tm,
        cast(end_effective_dt_tm as timestamp(6)) as end_effective_dt_tm

    from source

)

select * from renamed