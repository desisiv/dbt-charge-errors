{{
    config(
        materialized='incremental',
        unique_key='charge_item_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('stg_susp_chrg') }}
    
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    -- (uses >= to include records arriving later on the same day as the last run of this model)
    where updt_dt_tm >= (select max(updt_dt_tm) from {{ this }})

    {% endif %}
)

select * from source