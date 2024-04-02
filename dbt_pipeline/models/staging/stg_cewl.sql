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
        and (Err.TransStsMne = ''Err'' or (Err.TransStsMne != ''Err'' and Err.RslvDate >= DateAdd(m,-12,GetDate())))
    '))
)

select 
    chgerrobjid,
    cast(LastCngUTCDTime as timestamp(6)) as LastCngUTCDTime,
    cast(RslvDate as timestamp(6)) as RslvDate,
    cast(rcvowner as varchar) as rcvowner,
    cast(svcprov as varchar) as svcprov,
    cast(entsrcname as varchar) as entsrcname,
    cast(batchid as varchar) as batcchid,
    cast(TransStsMne as varchar) as TransStsMne,
    cast(transtype as varchar) as transtype,
    cast(ErrCd as varchar) as ErrCd,
    cast(errtext as varchar) as errtext,
    cast(firsterrdtime as timestamp(6)) as firsterrdttime,
    cast(mrn as varchar) as mrn,
    cast(encid as varchar) as encid,
    cast(ecdno as varchar) as ecdno,
    cast(spsid as varchar) as spsid,
    cast(spsissuer as varchar) as spsissuer,
    cast(svcid as varchar) as svcid,
    cast(svcidissid as varchar) as svcidissid,
    cast(svcname as varchar) as svcname,
    cast(ovrdsvcname as varchar) as ovrdsvcname,
    cast(transcommonid as varchar) as transcommonid,
    cast(entextrnid as varchar) as entextrnid,
    cast(ordno as varchar) as ordno,
    cast(servicedate as timestamp(6)) as servicedate,
    qty,
    rxqty,
    dseqty,
    ndcqty,
    extpriceamt,
    cast(priceovrdind as integer) as priceovrdind,
    cast(encprov as varchar) as encprov,
    cast(enctype as varchar) as enctype,
    cast(encstrdtime as timestamp(6)) as encstrdtime,
    cast(encstpdtime as timestamp(6)) as encstpdtime,
    cast(encsts as varchar) as encsts,
    cast(encloc as varchar) as encloc,
    cast(clinsvc as varchar) as clinsvc,
    cast(enccreuser as varchar) as enccreuser,
    cast(errcredtime as timestamp(6)) as errcredtime,
    cast(checkinuser as varchar) as checkinuser,
    cast(ignorersn as varchar) as ignorersn,
    cast(ignoredate as timestamp(6)) as ignoredate,
    cast(entproccdval as varchar) as entproccdval,
    cast(entchgmodfval as varchar) as entchgmodfval,
    cast(chgstpdtime as timestamp(6)) as chgstpdtime,
    rvusurgeryflag,
    placevalue,
    cast(labonly as varchar) as labonly,
    cast(fhcchg as varchar) as fhcchg,
    billinglocation,
    cast(actingaspcp as varchar) as actingaspcp,
    cast(perfhpid as varchar) as perfhpid,
    cast(perfhp as varchar) as perfhp,
    cast(ordhpid as varchar) as ordhpid,
    cast(ordhp as varchar) as ordhp,
    cast(supvhpid as varchar) as supvhpid,
    cast(supvhp as varchar) as supvhp,
    cast(refhpid as varchar) as refhpid,
    cast(refhp as varchar) as refhp,
    cast(primhlthplan as varchar) as primhlthplan,
    cast(primpayor as varchar) as primpayor,
    cast(sechlthplan as varchar) as sechlthplan,
    cast(secpayor as varchar) as secpayor,
    cast(glucose as varchar) as glucose,
    cast(lab as varchar) as lab,
    cast(pathology as varchar) as pathology,
    cast(if(position('MILNCHG' in cast(entextrnid as varchar)) > 0, regexp_replace(cast(entextrnid as varchar),'[^0-9.]'), '0') as decimal(38)) as interface_charge_id
from source
