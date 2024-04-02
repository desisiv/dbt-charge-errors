with source as (

    {#-
    Normally we would select from the table here,
    but we are using Trino to load our data in this project
    #}
    select * from table({{ var('dss_catalog') }}.system.query(query => '
        Select Distinct
            Err.ChgErrObjID, 
            Err.LastCngUTCDTime as LastCngUTCDTime,
            Err.RslvDate as RslvDate,
            Coalesce(HPO.RcvOwnerHPOId, SP.RcvOwnerHPOId) as RcvOwner,  
            Coalesce(HPO.ShortName, SP.ShortName) as SvcProv, 
            Err.EntSrcName as EntSrcName, 
            Err.BatchId as BatchId,
            Err.TransStsMne as TransStsMne,
            Err.TransTypeMne as TransType,
            Err.ErrCd as ErrCd,
            Err.ErrText as ErrText,
            Err.FirstErrDTime as FirstErrDTime, 
            Coalesce(Err.EncRpt_EncMRN, ER.EncMRN, ECD.EncMRN, Enc.EncMRN) as MRN,  
            Coalesce(Err.EncId, Err.EncRpt_EncId, Err.EncRpt_IntfcEncId, ER.EncId, Enc.EncId) as EncID, 
            Coalesce(Err.ECDNo, Err.EncRpt_ECDNo, ER.ECDNo, ECD.ECDNo, Enc.ECDNo) as ECDNo,
            Coalesce(Err.SvcProvSvcId, CF.SvcProvSvcId, SPSM.SvcProvSvcId) as SPSID,
            Err.SvcProvIdIssId as SPSIssuer,
            Svc.SvcId as SvcId,
            Err.SvcIdIssId as SvcIdIssId, 
            Svc.ShortName as SvcName, 
            Err.OvrdSvcName as OvrdSvcName, 
            Err.TransCommonId as TransCommonId, 
            Err.EntExtrnId as EntExtrnId, 
            Err.OrdNo as OrdNo, 
            err.ChgStrDTime as ServiceDate,
            Err.Qty, 
            Err.RxQty, 
            Err.DseQty, 
            Err.NDCQty, 
            Err.ExtPriceAmt, 
            Err.PriceOvrdInd as PriceOvrdInd,
            Err.EncRpt_EncProvHPOId as EncProv, 
            Err.EncRpt_EncTypeMne as EncType, 
            Err.EncRpt_EncStrDTime as EncStrDTime,
            Err.EncRpt_EncStpDTime as EncStpDTime,
            Err.EncRpt_EncStsMne as EncSts, 
            Err.EncRpt_EncLocId as EncLoc, 
            Err.EncRpt_LastClinSvcMne as ClinSvc, 
            Err.EncRpt_CreUserId as EncCreUser,
            Err.ErrCreDTime as ErrCreDTime,
            Err.EncRpt_CheckInUserId as CheckInUSer, 
            Err.IgnoreRsnMne as IgnoreRsn, 
            Err.IgnoreDate as IgnoreDate,
            Err.EntProcCdVal as EntProcCdVal, 
            Err.EntChgModfVal as EntChgModfVal, 
            Err.ChgStpDTime as ChgStpDTime,
            UD.RVUSURGERYFLAG as RVUSURGERYFLAG, 
            UD.PlaceValue as PlaceValue, 
            UD.LABONLY as LABONLY, 
            UD.FHCCHG as FHCCHG, 
            UD.BillingLocation as BillingLocation, 
            UD.ACTINGASPCP as ACTINGASPCP,
            Err.PerfHlthProfId as PerfHPID, 
            PerfHP.RptName as PerfHP,
            Err.OrdHlthProfId as OrdHPID, 
            OrdHP.RptName as OrdHP, 
            Err.SupvHlthProfId as SupvHPID, 
            SupvHP.RptName as SupvHP, 
            Err.RefHlthProfId as RefHPID, 
            RefHP.RptName as RefHP, 
            HP.ShortName as PrimHlthPlan, 
            PM.ShortName as PrimPayor, 
            HP2.ShortName as SecHlthPlan, 
            PM2.ShortName as SecPayor,
            Case
                When Err.SvcProvSvcId in (''1070838'',''1105858'') then ''Y''
                Else ''N''
            End as Glucose,
            Case 
                When HPO.Shortname like ''%Laboratory%'' Then ''Y''
                When Err.SvcProvSvcId in (''1070838'',''1105858'') then ''Y''
                Else ''N''
            End as Lab,
            Case 
                When Svc.ShortName like ''%Path%'' Then ''Y''
                When Svc.ShortName like ''%Cyto%'' then ''Y''
                When Svc.ShortName like ''%G0123%'' then ''Y''
                When Svc.ShortName like ''%G0124%'' then ''Y''
                When Svc.ShortName like ''%Thin Prep%'' then ''Y''
                Else ''N''
            End as Pathology

        From smsdss.ChgErrEncRptV             Err
        Left Join dbo.ChgErrRptV              CE     on CE.ChgErrObjID = Err.ChgErrObjID
        Left Join smsdss.ChgFctV              CF     on CF.ChgObjId = Err.ChgObjId
        Left Join smsdss.HPOMstrV             HPO    on HPO.HPOExtrnId = Err.SvcIdIssId
        Left Join smsdss.HPOMstrV             SP     on SP.HPOObjId = Err.SvcProvHPOObjId

        Left Join smsdss.SvcMstrV             Svc    on Svc.SvcObjid = Err.SvcObjId
        Left Join smsdss.SvcProvSvcMstrV      SPSM   on Err.SvcObjId = SPSM.SvcObjId and Err.SvcProvHPOObjId = SPSM.SvcProvHPOObjId and SPSM.StpDate IS NULL   
        
        Left Join smsdss.EncRptV              ER     on ER.EncObjId = Err.EncObjId
        Left Join smsdss.EncRptV              Enc    on Enc.EncId = Err.EncId
        Left Join smsdss.EncRptV              ECD    on ECD.ECDNo = Err.ECDNo

        Left Join smsdss.PyrHlthPlanMstrV     HP     on HP.PyrHlthPlanObjId = Err.EncRpt_PrimPyrHlthPlanObjId
        Left Join smsdss.PyrMstrV             PM     on PM.PyrObjId= Err.EncRpt_PrimPyrObjId
        Left Join smsdss.EncPolMemRptV        I2     on I2.EncObjId = Err.EncObjId and I2.RankNo = 2
        Left Join smsdss.PolMemRptV           Pol    on Pol.PolMemObjId = I2.PolMemObjId
        Left Join smsdss.PyrHlthPlanMstrV     HP2    on HP2.PyrHlthPlanObjId = Pol.PyrHlthPlanObjId
        Left Join smsdss.PyrMstrV             PM2    on PM2.PyrObjId= Pol.PyrObjId

        Left Join dbo.HlthProfId              HPID   on HPID.ExtrnId = Err.PerfHlthProfId and HPID.IdDefobjid = 1136051720
        Left Join smsdss.HlthProfMstrV        PerfHP on PerfHP.HlthprofObjId = HPID.HlthProfObjId
        Left Join smsdss.HlthProfMstrV        RefHP  on RefHP.HlthprofObjId = Err.EncRpt_RefHlthProfObjId

        Left Join dbo.HlthProfId              HPID2  on HPID2.ExtrnId = Err.OrdHlthProfId and HPID2.IdDefobjid = 1136051720
        Left Join smsdss.HlthProfMstrV        OrdHP  on OrdHP.HlthprofObjId = HPID2.HlthProfObjId

        Left Join dbo.HlthProfId              HPID3  on HPID3.ExtrnId = Err.SupvHlthProfId and HPID3.IdDefobjid = 1136051720
        Left Join smsdss.HlthProfMstrV        SupvHP on SupvHP.HlthprofObjId = HPID3.HlthProfObjId

        Left Join dbo.vw_ChgCommonUserData    UD     on UD.ChgCommonObjId = Err.ChgCommonObjId

        Where (substring(Err.EncRpt_EncStrPtName,1,7) <> ''ZZZCERN'' or Err.EncRpt_EncStrPtName is Null)
        and (PerfHP.RptName Not Like ''%zzz%'' or PerfHP.RptName is Null)
        and (OrdHP.RptName Not Like ''%zzz%'' or OrdHP.RptName is Null)
        and (SupvHP.RptName Not Like ''%zzz%'' or SupvHP.RptName is Null)
        and (RefHP.RptName Not Like ''%zzz%'' or RefHP.RptName is Null)
        and (CE.TransStsMne = ''Err'' or CE.TransStsMne is NULL)
        and (Err.TransStsMne = ''Err'' or (Err.TransStsMne != ''Err'' and Err.RslvDate >= DateAdd(m,-12,GetDate())))

        Union

        select Distinct 
            Err.ChgErrObjID,
            Err.LastCngUTCDTime as LastCngUTCDTime,
            Err.RslvDate as RslvDate,
            Coalesce(HPO.RcvOwnerHPOId, SP.RcvOwnerHPOId) as RcvOwner,
            Coalesce(HPO.ShortName, SP.ShortName) as SvcProv, 
            Err.EntSrcName as EntSrcName,
            Err.BatchId as BatchId,
            Err.TransStsMne as TransStsMne,
            Err.TransTypeMne as TransType, 
            Err.ErrCd as ErrCd,
            Err.ErrText as ErrText, 
            Err.FirstErrDTime as FirstErrDTime, 
            Coalesce(CEE.EncRpt_EncMRN, ER.EncMRN, ECD.EncMRN, Enc.EncMRN) as MRN,  
            Coalesce(Err.EncId, CEE.EncRpt_EncId, CEE.EncRpt_IntfcEncId, ER.EncId, Enc.EncId) as EncID,
            Coalesce(Err.ECDNo, CEE.EncRpt_ECDNo, ER.ECDNo, ECD.ECDNo, Enc.ECDNo) as ECDNo,
            Coalesce(Err.SvcProvSvcId, CF.SvcProvSvcId, SPSM.SvcProvSvcId) as SPSID,
            Err.SvcProvIdIssId as SPSIssuer, 
            Svc.SvcId as SvcId,
            Err.SvcIdIssId as SvcIdIssId, 
            Svc.ShortName as SvcName,
            Err.OvrdSvcName as OvrdSvcName, 
            Err.TransCommonId as TransCommonId, 
            Err.EntExtrnId as EntExtrnId, 
            Err.OrdNo as OrdNo, 
            err.ChgStrDTime as ServiceDate, 
            Err.Qty, 
            Err.RxQty, 
            Err.DseQty, 
            Err.NDCQty, 
            Err.ExtPriceAmt, 
            Err.PriceOvrdInd as PriceOvrdInd,
            ER.EncProvHPOId as EncProv, 
            ER.EncTypeMne as EncType, 
            ER.EncStrDTime as EncStrDTime,
            ER.EncStpDTime as EncStpDTime,
            ER.EncStsMne as EncSts, 
            ER.EncLocId as EncLoc, 
            ER.LastClinSvcMne as ClinSvc, 
            ER.CreUserId as EncCreUser,
            Err.ErrCreDTime as ErrCreDTime,
            ER.CheckInUserId as CheckInUSer, 
            Err.IgnoreRsnMne as IgnoreRsn, 
            Err.IgnoreDate as IgnoreDate,
            Err.EntProcCdVal as EntProcCdVal, 
            Err.EntChgModfVal as EntChgModfVal, 
            Err.ChgStpDTime as ChgStpDTime,
            UD.RVUSURGERYFLAG as RVUSURGERYFLAG,
            UD.PlaceValue as PlaceValue, 
            UD.LABONLY as LABONLY, 
            UD.FHCCHG as FHCCHG, 
            UD.BillingLocation as BillingLocation, 
            UD.ACTINGASPCP as ACTINGASPCP,
            Err.PerfHlthProfId as PerfHPID, 
            PerfHP.RptName as PerfHP,
            Err.OrdHlthProfId as OrdHPID, 
            OrdHP.RptName as OrdHP, 
            Err.SupvHlthProfId as SupvHPID, 
            SupvHP.RptName as SupvHP, 
            Err.RefHlthProfId as RefHPID, 
            RefHP.RptName as RefHP, 
            HP.ShortName as PrimHlthPlan, 
            PM.ShortName as PrimPayor, 
            HP2.ShortName as SecHlthPlan, 
            PM2.ShortName as SecPayor, 
            Case
                When Err.SvcProvSvcId in (''1070838'',''1105858'') then ''Y''
                Else ''N''
            End as Glucose,
            Case 
                When HPO.Shortname like ''%Laboratory%'' Then ''Y''
                When Err.SvcProvSvcId in (''1070838'',''1105858'') then ''Y''
                Else ''N''
            End as Lab,
            Case 
                When Svc.ShortName like ''%Path%'' Then ''Y''
                When Svc.ShortName like ''%Cyto%'' then ''Y''
                When Svc.ShortName like ''%G0123%'' then ''Y''
                When Svc.ShortName like ''%G0124%'' then ''Y''
                When Svc.ShortName like ''%Thin Prep%'' then ''Y''
                Else ''N''
            End as Pathology

        From dbo.ChgErrRptV                   Err
        Left Join smsdss.ChgErrEncRptV        CEE    on CEE.ChgErrObjID = Err.ChgErrObjID
        Left Join smsdss.ChgFctV              CF     on CF.ChgObjId = Err.ChgObjId
        Left Join smsdss.HPOMstrV             HPO    on HPO.HPOExtrnId = Err.SvcIdIssId
        Left Join smsdss.HPOMstrV             SP     on SP.HPOObjId = Err.SvcProvHPOObjId

        Left Join smsdss.SvcMstrV             Svc    on Svc.SvcObjid = Err.SvcObjId
        Left Join smsdss.SvcProvSvcMstrV      SPSM   on Err.SvcObjId = SPSM.SvcObjId and Err.SvcProvHPOObjId = SPSM.SvcProvHPOObjId and SPSM.StpDate IS NULL   
        
        Left Join smsdss.EncRptV              ER     on ER.EncObjId = Err.EncObjId
        Left Join smsdss.EncRptV              Enc    on Enc.EncId = Err.EncId
        Left Join smsdss.EncRptV              ECD    on ECD.ECDNo = Err.ECDNo

        Left Join smsdss.PyrHlthPlanMstrV     HP     on HP.PyrHlthPlanObjId = ER.PrimPyrHlthPlanObjId
        Left Join smsdss.PyrMstrV             PM     on PM.PyrObjId= ER.PrimPyrObjId
        Left Join smsdss.EncPolMemRptV        I2     on I2.EncObjId = Err.EncObjId and I2.RankNo = 2
        Left Join smsdss.PolMemRptV           Pol    on Pol.PolMemObjId = I2.PolMemObjId
        Left Join smsdss.PyrHlthPlanMstrV     HP2    on HP2.PyrHlthPlanObjId = Pol.PyrHlthPlanObjId
        Left Join smsdss.PyrMstrV             PM2    on PM2.PyrObjId= Pol.PyrObjId

        Left Join dbo.HlthProfId              HPID   on HPID.ExtrnId = Err.PerfHlthProfId and HPID.IdDefobjid = 1136051720
        Left Join smsdss.HlthProfMstrV        PerfHP on PerfHP.HlthprofObjId = HPID.HlthProfObjId
        Left Join smsdss.HlthProfMstrV        RefHP  on RefHP.HlthprofObjId = ER.RefHlthProfObjId

        Left Join dbo.HlthProfId              HPID2  on HPID2.ExtrnId = Err.OrdHlthProfId and HPID2.IdDefobjid = 1136051720
        Left Join smsdss.HlthProfMstrV        OrdHP  on OrdHP.HlthprofObjId = HPID2.HlthProfObjId

        Left Join dbo.HlthProfId              HPID3  on HPID3.ExtrnId = Err.SupvHlthProfId and HPID3.IdDefobjid = 1136051720
        Left Join smsdss.HlthProfMstrV        SupvHP on SupvHP.HlthprofObjId = HPID3.HlthProfObjId

        Left Join dbo.vw_ChgCommonUserData    UD     on UD.ChgCommonObjId = Err.ChgCommonObjId

        Where (substring(ER.EncStrPtName,1,7) <> ''ZZZCERN'' or ER.EncStrPtName is Null)
        and (PerfHP.RptName Not Like ''%zzz%'' or PerfHP.RptName is Null)
        and (OrdHP.RptName Not Like ''%zzz%'' or OrdHP.RptName is Null)
        and (SupvHP.RptName Not Like ''%zzz%'' or SupvHP.RptName is Null)
        and (RefHP.RptName Not Like ''%zzz%'' or RefHP.RptName is Null)
        and (Err.TransStsMne = ''Err'')
    '))
),
cewl_base as (
    select 
        s.chgerrobjid,
        {{ serv_prov_flag('s.entproccdval','sp.metadata1') }} as sp_flag,
        {{ crd_indicator('s.errtext','s.transstsmne')}} as crd_yn,
        count_if(s.transstsmne = 'Err') over(partition by s.entextrnid, s.transstsmne) as group_cnt,
        max({{ crd_indicator('s.errtext','s.transstsmne')}}) over(partition by s.entextrnid, s.transstsmne) as group_crd_ind,
        count_if(s.TransStsMne = 'Err' and s.transtype ='Chg') over(partition by s.entextrnid, s.transstsmne) as group_chg_cnt,
        count_if(s.TransStsMne = 'Err' and s.transtype ='Chg Rvsl') over(partition by s.entextrnid, s.transstsmne) as group_chg_rvsl_cnt,
        cast(lag(s.entextrnid) over(partition by s.entextrnid, s.transstsmne order by s.transtype) as varchar) as prev_entextrnid,
        cast(lag(s.transtype) over(partition by s.entextrnid, s.transstsmne order by s.transtype) as varchar) as prev_transtype,
        case
            when s.firsterrdtime < timestamp '2022-03-31 23:59:59 {{ time_zone }}' then 'Y'
            else 'N'
        end as firsterr,
        case
            when s.firsterrdtime < timestamp '2022-07-04 23:59:59 {{ time_zone }}' then 'Y'
            else 'N'
        end as firsterrjuly,
        case
            when s.servicedate < timestamp '2022-03-31 23:59:59 {{ time_zone }}' then 'Y'
            else 'N'
        end as dosmarch,
        case
            when tp.lookup_field is not null then 'Y'
            else 'N'
        end as test_patient,
        case
            when ltp.lookup_field is not null then 'Y'
            else 'N'
        end as lab_test_patient,
        case
            when substring(s.spsid,1,1) = '7'
            and length(s.spsid) = 7
            then 'Y'
            else 'N'
        end as pharmacy,
        bca.metadata1 as cat_archived, -- **REVIEW** likely need something more sustainable
        case
            when st.metadata1 is null then 'Review'
            else st.metadata1
        end as SvcType, -- **REVIEW** can we look this up in Soarian
        cast(s.LastCngUTCDTime as timestamp(6) with time zone) as LastCngUTCDTime,
        cast(s.RslvDate as timestamp(6) with time zone) as RslvDate,
        cast(s.rcvowner as varchar) as rcvowner,
        cast(s.svcprov as varchar) as svcprov,
        cast(s.entsrcname as varchar) as entsrcname,
        cast(s.batchid as varchar) as batchid,
        cast(s.TransStsMne as varchar) as TransStsMne,
        cast(s.transtype as varchar) as transtype,
        cast(s.ErrCd as varchar) as ErrCd,
        cast(s.errtext as varchar) as errtext,
        cast(s.firsterrdtime as timestamp(6) with time zone) as firsterrdttime,
        cast(s.mrn as varchar) as mrn,
        cast(s.encid as varchar) as encid,
        cast(s.ecdno as varchar) as ecdno,
        cast(s.spsid as varchar) as spsid,
        cast(s.spsissuer as varchar) as spsissuer,
        cast(s.svcid as varchar) as svcid,
        cast(s.svcidissid as varchar) as svcidissid,
        cast(s.svcname as varchar) as svcname,
        cast(s.ovrdsvcname as varchar) as ovrdsvcname,
        cast(s.transcommonid as varchar) as transcommonid,
        cast(s.entextrnid as varchar) as entextrnid,
        cast(s.ordno as varchar) as ordno,
        cast(s.servicedate as timestamp(6) with time zone) as servicedate,
        s.qty,
        s.rxqty,
        s.dseqty,
        s.ndcqty,
        s.extpriceamt,
        cast(s.priceovrdind as integer) as priceovrdind,
        cast(s.encprov as varchar) as encprov,
        cast(s.enctype as varchar) as enctype,
        cast(s.encstrdtime as timestamp(6) with time zone) as encstrdtime,
        cast(s.encstpdtime as timestamp(6) with time zone) as encstpdtime,
        cast(s.encsts as varchar) as encsts,
        cast(s.encloc as varchar) as encloc,
        cast(s.clinsvc as varchar) as clinsvc,
        cast(s.enccreuser as varchar) as enccreuser,
        cast(s.errcredtime as timestamp(6) with time zone) as errcredtime,
        cast(s.checkinuser as varchar) as checkinuser,
        cast(s.ignorersn as varchar) as ignorersn,
        cast(s.ignoredate as timestamp(6) with time zone) as ignoredate,
        cast(s.entproccdval as varchar) as entproccdval,
        cast(s.entchgmodfval as varchar) as entchgmodfval,
        cast(s.chgstpdtime as timestamp(6) with time zone) as chgstpdtime,
        s.rvusurgeryflag,
        s.placevalue,
        cast(s.labonly as varchar) as labonly,
        cast(s.fhcchg as varchar) as fhcchg,
        s.billinglocation,
        cast(s.actingaspcp as varchar) as actingaspcp,
        cast(s.perfhpid as varchar) as perfhpid,
        cast(s.perfhp as varchar) as perfhp,
        cast(s.ordhpid as varchar) as ordhpid,
        cast(s.ordhp as varchar) as ordhp,
        case when s.perfhpid = s.ordhpid then 'Y' else 'N' end as perf_ord_same_hp,
        cast(s.supvhpid as varchar) as supvhpid,
        cast(s.supvhp as varchar) as supvhp,
        cast(s.refhpid as varchar) as refhpid,
        cast(s.refhp as varchar) as refhp,
        cast(s.primhlthplan as varchar) as primhlthplan,
        cast(s.primpayor as varchar) as primpayor,
        cast(s.sechlthplan as varchar) as sechlthplan,
        cast(s.secpayor as varchar) as secpayor,
        cast(s.glucose as varchar) as glucose,
        cast(s.lab as varchar) as lab,
        cast(s.pathology as varchar) as pathology,
        cast(if(position('MILNCHG' in cast(s.entextrnid as varchar)) > 0, regexp_replace(cast(s.entextrnid as varchar),'[^0-9.]'), '0') as decimal(38)) as interface_charge_id
    from source s
    left join {{ var('analytics_catalog') }}.{{ var('analytics_schema')}}.cewlxref sp -- SvcProv
    on sp.lookup_field = s.svcprov
    and sp.lookup_field_type = 'SvcProv'
    left join {{ var('analytics_catalog') }}.{{ var('analytics_schema')}}.cewlxref tp --test patient mrn
    on tp.lookup_field = s.mrn
    and tp.lookup_field_type = 'Test Patients MRN'
    left join {{ var('analytics_catalog') }}.{{ var('analytics_schema')}}.cewlxref ltp --lab test patient ecd
    on ltp.lookup_field = s.ecdno
    and ltp.lookup_field_type = 'Lab Test Patient ECD'
    left join {{ var('analytics_catalog') }}.{{ var('analytics_schema')}}.cewlxref bca -- BatchID cat.archived
    on bca.lookup_field = s.batchid
    and bca.lookup_field_type = 'BatchID'
    left join {{ var('analytics_catalog') }}.{{ var('analytics_schema')}}.cewlxref st -- SvcType
    on st.lookup_field = s.svcid
    and st.lookup_field_type = 'SID'
)

select
    cb.*,
    case
        when concat(cb.transtype,cb.entextrnid) = concat(cb.prev_transtype,cb.prev_entextrnid)
        then 0
        else 1
    end as unique_cnt
    -- FOR SOME REASON THE BELOW DOESN'T WORK.  NEED TO INVESTIGATE WHY
    -- count_if(concat(cb.transtype,cb.entextrnid) <> concat(cb.prev_transtype,cb.prev_entextrnid))
    --     over(partition by cb.entextrnid, cb.transstsmne) as group_unique_cnt
from cewl_base cb
