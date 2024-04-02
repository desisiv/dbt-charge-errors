{% set sp_linking_err_list = 
    [
    "Charge posting is not allowed to Canceled or No Show Encounters for this service Provider",
    "Failed to retrieve service pricing rule.",
    "Performing Encounter is a scheduled encounter.  Automatic checkin of this encounter failed.",
    "Quantity must be greater than zero.",
    "Quantity must be within limit 99999.0.",
    "Service ID Type is required.",
    "Service provider does not offer this service on performance date specified.",
    "The charge quantity must be a whole number for this service.",
    ]
%}

{% set daily_workflow_err_list =
    [
    "Two or more of the entered Diagnosis Codes have the same value.",
    "Quantity must be greater than zero.",
    "Service is not active.",
    "The entered charge amount (price override) exceeds the maximum that is allowed.",
    "Service Provider has no active Receivable Owner for the Service Performance Date.",
    ]
%}
with prsnl as (
    select * from {{ ref('stg_prsnl') }}
),
prsnl_alias as (
    select * from {{ ref('stg_prsnl_alias') }}
),
code_value as (
    select * from {{ ref('stg_code_value') }}
),
cewl as (
    select * from {{ ref('stg_cewl_v2') }}
),
final as (
    select
        cewl.*,
        prsnl_alias.prsnl_alias_type_cd as personnel_type_cd,
        prsnl.name_full_formatted as perf_prsnl_name,
        code_value.display as perf_prsnl_position,
        sum(cewl.unique_cnt) over(partition by cewl.entextrnid, cewl.transstsmne) as group_unique_cnt
    from cewl
    left join prsnl_alias
      on prsnl_alias.alias = cewl.perfhpid
     and cewl.transstsmne = 'Err'
     and prsnl_alias.prsnl_alias_type_cd = (select code_value.code_value from code_value where code_value.code_set = 320 and code_value.cki = 'CKI.CODEVALUE!6664') -- ORGANIZATION DOCTOR
     and prsnl_alias.active_ind = 1
     and prsnl_alias.beg_effective_dt_tm < current_timestamp
     and prsnl_alias.end_effective_dt_tm > current_timestamp
    left join prsnl
      on prsnl.person_id = prsnl_alias.person_id
     and prsnl.active_ind = 1
     and prsnl.beg_effective_dt_tm < current_timestamp
     and prsnl.end_effective_dt_tm > current_timestamp
    left join code_value
      on code_value.code_value = prsnl.position_cd
)

select
    f.*,
    case
        -- "No Category Identified" - step 0 exclude all sp_flag = 'Lab'
        when f.sp_flag = 'Lab'
        then 'Analysis Required'
        -- "Credit Review Multi" - step 2 category spreadsheet
        when f.firsterrjuly = 'N'
         and f.sp_flag != 'Lab'
         and f.group_crd_ind = 'Y'
         and f.group_cnt = f.group_unique_cnt
        then 'Credit Review Multi'
        -- "Soarian Room Charge" - step 3 category spreadsheet
        when f.entsrcname in ('w0awbulkuser','w0awasyncuser')
        then 'Soarian Room Charge'
        -- "Infusion Chemo CHI" - step 4 category spreadsheet
        when f.errtext = 'No valid Service having the provided ID exists.'
         and f.svcprov = 'Infusion Chemo CHI'
        then 'Infusion Chemo CHI'
        -- "Room/Bed Review" - step 5 category spreadsheet
        when f.errtext = 'Room charge already posted for the specified date.'
        then 'Room/Bed - Review'
        -- "Encounter Error" - step 6 category spreadsheet
        when f.errtext like '%Encounter%'
          or f.errtext like '%encounter%'
        then 'Encounter Error'
        -- "Prof Chg - No Performing HP" - step 7 category spreadsheet
        when f.errtext =  'Performing Health Professional ID Issuer and Performing Health Professional ID Type are required.'
        then 'Prof Chg - No Performing HP'
        -- "Therapy Provider" - step 9 category spreadsheet
        when substring(f.errtext,1,6) = 'Health'
         and substring(f.svcprov,1,8) = 'Physical'
         and f.perf_ord_same_hp = 'Y'
        then 'Therapy Provider'
        -- "Health Professional" - step 10 category spreadsheet
        when substring(f.errtext,1,6) = 'Health'
         and substring(f.svcprov,1,8) = 'Physical'
         and f.perf_ord_same_hp = 'N'
        then 'Health Professional'
        -- "Rad Tech - HP Audit" - step 11 category spreadsheet
        when substring(f.errtext,1,6) = 'Health'
         and f.svcprov like '%Rad%'
         and f.svcprov not like '%InterventionRad%'
         and f.svcprov not like '%Rad Onc%'
         and f.svcprov not like '%System Rad Prof%'
         and f.perf_ord_same_hp = 'Y'
        then 'Rad Tech - HP Audit'
        -- "Health Professional" - step 12 category spreadsheet
        when substring(f.errtext,1,6) = 'Health'
         and f.svcprov like '%Rad%'
         and f.perf_ord_same_hp = 'N'
        then 'Health Professional'
        -- "Health Professional" - step 13 category spreadsheet
        when substring(f.errtext,1,6) = 'Health'
        then 'Health Professional'
        -- "Supply Pricing" - step 14 category spreadsheet
        when f.errtext = 'Failed to retrieve service pricing rule.'
         and substring(f.spsid,1,4) = '7850'
         and f.pharmacy = 'Y'
        then 'Supply Pricing'
        -- "No Cost Center" - step 15 category spreadsheet
        when f.errtext = 'Service ID Type is required.'
        then 'No Cost Center'
        -- "Svc Catalog - Build/Link" - step 16 category spreadsheet
        when f.errtext = 'No valid Service having the provided ID exists.'
         and substring(f.spsid,1,3) = '791'
         and f.svcname is null
        then 'Svc Catalog - Build/Link'
        -- "Svc Catalog - SP Linking" - step 17 category spreadsheet
        when f.errtext = 'No valid Service having the provided ID exists.'
         and substring(f.spsid,1,3) = '791'
         and f.svcname is not null
        then 'Svc Catalog - SP Linking'
        -- "Svc Catalog - SP Linking" - steps 18 through 25 category spreadsheet
        {% for sp_linking_err in sp_linking_err_list %}
        when f.errtext = '{{ sp_linking_err }}'
         and substring(f.spsid,1,1) = '7'
         and length(f.spsid) = 7
         and f.svcname is not null
        then 'Svc Catalog - SP Linking'
        {% endfor %}
        -- "Supply Pricing" - step 26 category spreadsheet
        when f.errtext = 'Failed to retrieve service pricing rule.'
         and f.sp_flag != 'Amb'
         and (
            f.svcprov like 'CIRV%'
            or f.svcprov like 'EW OP Surgery%'
            or f.svcprov like 'Surgical Svcs%'
            )
        then 'Supply Pricing'
        -- "Encounter Allocation" - step 27 category spreadsheet
        when f.errtext = 'Failed to retrieve service pricing rule.'
         and f.sp_flag != 'Amb'
         and f.encloc like '%CTR'
        then 'Encounter Allocation'
        -- "Pricing Review" - step 28 category spreadsheet
        when f.errtext = 'Failed to retrieve service pricing rule.'
         and f.sp_flag != 'Amb'
        then 'Pricing Review'
        -- "Missing Place Value-1" - step 29 (part 1 - pre 7/27) category spreadsheet
        when f.errtext = 'Failed to retrieve service pricing rule.'
         and f.sp_flag = 'Amb'
         and f.servicedate < timestamp '2022-07-27 23:59:59 {{ time_zone }}'
         and f.encloc in ('EC center','ECO','EPT center','RLK MOB')
         and f.placevalue is null
        then 'Missing Place Value-1'
        -- "Missing Place Value-1" - step 29 (part 2 - post 7/27) category spreadsheet
        when f.errtext = 'Failed to retrieve service pricing rule.'
         and f.sp_flag = 'Amb'
         and f.servicedate > timestamp '2022-07-27 23:59:59 {{ time_zone }}'
         and f.encloc in ('ECCTR','ECOCTR','EPTCTR','LDYBCTR','RLKCTRB')
         and f.placevalue is null
        then 'Missing Place Value-1'
        -- "T-code on MOA/Rev Routing" - step 30 category spreadsheet
        when f.errtext = 'Failed to retrieve service pricing rule.'
         and f.sp_flag = 'Amb'
         and f.encloc in ('RLKCTRA','LDYCTRA')
         and f.spsid like '%T%'
        then 'T-code on MOA/Rev Routing'
        -- "Pricing Review" - step 31 category spreadsheet
        when f.errtext = 'Failed to retrieve service pricing rule.'
         and f.sp_flag = 'Amb'
        then 'Pricing Review'
        -- "Questions" - step 32 category spreadsheet
        when f.errtext = 'Unable to determine service as a valid Service Provider is missing.'
         and f.svcprov is null
        then 'Questions'
        -- "No Cost Center" - step 33 category spreadsheet
        when f.errtext = 'Service ID Type is required.'
         and f.svcprov is null
        then 'No Cost Center'
        -- "Daily Workflow" - steps 34 through 38 category spreadsheet
        {% for daily_workflow_err in daily_workflow_err_list %}
        when f.errtext = '{{ daily_workflow_err }}'
        then 'Daily Workflow'
        {% endfor %}
        -- "Svc Catalog - SP Linking" - step 39
        when f.errtext = 'No valid Service having the provided ID exists.'
         and f.svcname is not null
        then 'Svc Catalog - SP Linking'
        -- "Svc Catalog - Build/Link" - step 40 category spreadsheet
        when f.errtext = 'No valid Service having the provided ID exists.'
         and f.sp_flag = 'Hosp'
         and f.svcname is null
        then 'Svc Catalog - Build/Link'
        -- "Svc Catalog - SP Linking" - step 41 category spreadsheet
        when f.errtext = 'No valid Service having the provided ID exists.'
         and f.sp_flag = 'Hosp'
         and f.svcname is not null
        then 'Svc Catalog - SP Linking'
        -- "Svc Catalog - Build/Link" - step 42 category spreadsheet
        when f.errtext = 'No valid Service having the provided ID exists.'
         and f.sp_flag = 'Amb'
         and f.svcname is null
        then 'Svc Catalog - Build/Link'
        -- "Svc Catalog - SP Linking" - step 43 category spreadsheet
        when f.errtext = 'No valid Service having the provided ID exists.'
         and f.sp_flag = 'Amb'
         and f.svcname is not null
        then 'Svc Catalog - SP Linking'
        -- "Chrg Date Review" - step 44 category spreadsheet
        when f.errtext = 'Service date must be less than or equal to current date.'
        then 'Chrg Date Review'
        -- "GL String Error" - step 45 category spreadsheet
        when f.errtext like '%GL Error%'
        then 'GL String Error'
        -- "Diagnosis Coding Review" - step 46 category spreadsheet
        when f.errtext like 'Invalid code%'
          or f.errtext like '%Diagnosis%'
        then 'Diagnosis Coding Review'
        -- "Analysis Required" anything left over
        else 'Analysis Required'
    end as category
from final f