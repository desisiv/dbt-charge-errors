with source as (

    {#-
    Normally we would select from the table here,
    but we are using Trino to load our data in this project
    #}
    select * from {{ var('mill_catalog') }}.{{ var('mill_schema') }}.charge

),

renamed as (

    select
        charge_item_id,
        activity_type_cd

    from source

)

select * from renamed