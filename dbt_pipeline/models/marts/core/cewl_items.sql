{{
    config(
        materialized='incremental',
        unique_key='ChgErrObjID',
        incremental_strategy='delete+insert',
    )
}}

with source as (
    select * from {{ ref('stg_cewl') }}
    
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    -- (uses >= to include records arriving later on the same day as the last run of this model)
    where LastCngUTCDTime >= (select max(LastCngUTCDTime) from {{ this }})

    {% endif %}
)

select * from source