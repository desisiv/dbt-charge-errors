with source as (

    {#-
    Normally we would select from the table here,
    but we are using Trino to load our data in this project
    #}
    select
        charge_item_id,
        charge_event_id,
        SuspenseReason,
        charge_description,
        process_flg,
        item_price,
        item_extended_price,
        item_quantity,
        service_dt_tm,
        updt_dt_tm,
        ext_i_reference_id,
        ext_i_reference_cont_cd,
        field1_id,
        bill_item_id,
        ext_description as BillItemDescription,
        Reference,
        ReferenceContributor,
        ProcessFlag
    from table({{ var('mill_catalog') }}.system.query(query => '
        select
            c.charge_item_id,
            ce.charge_event_id,
            cv3.display as SuspenseReason,
            c.charge_description,
            c.process_flg,
            cast(c.item_price as binary_double) as item_price,
            cast(c.item_extended_price as binary_double) as item_extended_price,
            cast(c.item_quantity as binary_double) as item_quantity,
            c.service_dt_tm,
            c.updt_dt_tm,
            ce.ext_i_reference_id,
            ce.ext_i_reference_cont_cd,
            cm.field1_id,
            c.bill_item_id,
            b.ext_description,
            cv.display as Reference,
            cv2.display as ReferenceContributor,
            (case when c.process_flg=1 then ''Suspended''
            when c.process_flg=2 then ''Review''
            when c.process_flg=3 then ''On Hold''
            when c.process_flg=4 then ''Manual''
            end) as ProcessFlag
        from charge c
        join charge_mod cm on cm.charge_item_id = c.charge_item_id and cm.active_ind = 1
        join charge_event ce on ce.charge_event_id = c.charge_event_id
        join code_value cv2 on cv2.code_value = ce.ext_i_reference_cont_cd
        join code_value cv3 on cv3.code_value = cm.field1_id
        join code_value cv4 on cv4.code_value = cm.charge_mod_type_cd and cv4.cdf_meaning = ''SUSPENSE'' and cv4.code_set = 13019
        left join code_value cv on cv.code_value = ce.ext_i_reference_id
        left join bill_item b on b.bill_item_id = c.bill_item_id
        where c.process_flg in (1,2,3,4)
        and c.active_ind = 1
    '))
)

select
    charge_item_id,
    charge_event_id,
    SuspenseReason,
    charge_description,
    process_flg,
    item_price,
    item_extended_price,
    item_quantity,
    cast(service_dt_tm as timestamp(6)) as service_dt_tm,
    cast(updt_dt_tm as timestamp(6)) as updt_dt_tm,
    ext_i_reference_id,
    ext_i_reference_cont_cd,
    field1_id,
    bill_item_id,
    BillItemDescription,
    Reference,
    ReferenceContributor,
    ProcessFlag
from source
