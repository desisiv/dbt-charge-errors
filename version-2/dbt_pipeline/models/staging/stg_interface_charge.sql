with source as (

    {#-
    Normally we would select from the table here,
    but we are using Trino to load our data in this project
    #}
    select * from {{ var('mill_catalog') }}.{{ var('mill_schema') }}.interface_charge

),

renamed as (

    select
        interface_charge_id,
        charge_item_id

    from source

)

select * from renamed