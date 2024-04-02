Below is the schema definition of the ChargeErrorFullExporttableV view from the Soarian DSS customer schema:

    Column      |     Type      | Extra | Comment 
-----------------+---------------+-------+---------
 chgerrobjid     | bigint        |       |         
 rcvowner        | char(8)       |       |         
 svcprov         | varchar(20)   |       |         
 entsrcname      | varchar(60)   |       |         
 batchid         | varchar(254)  |       |         
 transtype       | varchar(40)   |       |         
 errcd           | bigint        |       |         
 errtext         | varchar(512)  |       |         
 firsterrdtime   | timestamp(3)  |       |         
 mrn             | varchar(254)  |       |         
 encid           | varchar(254)  |       |         
 ecdno           | varchar(254)  |       |         
 spsid           | varchar(254)  |       |         
 spsissuer       | varchar(254)  |       |         
 svcid           | varchar(254)  |       |         
 svcidissid      | varchar(254)  |       |         
 svcname         | char(60)      |       |         
 ovrdsvcname     | varchar(60)   |       |         
 transcommonid   | varchar(254)  |       |         
 entextrnid      | varchar(254)  |       |         
 ordno           | char(16)      |       |         
 servicedate     | timestamp(3)  |       |         
 qty             | decimal(9,2)  |       |         
 rxqty           | decimal(9,4)  |       |         
 dseqty          | decimal(12,4) |       |         
 ndcqty          | decimal(16,3) |       |         
 extpriceamt     | decimal(16,2) |       |         
 priceovrdind    | smallint      |       |         
 encprov         | char(8)       |       |         
 enctype         | varchar(40)   |       |         
 encstrdtime     | timestamp(3)  |       |         
 encstpdtime     | timestamp(3)  |       |         
 encsts          | varchar(40)   |       |   
 encloc          | char(8)       |       |         
 clinsvc         | varchar(40)   |       |         
 enccreuser      | varchar(254)  |       |         
 errcredtime     | timestamp(3)  |       |         
 checkinuser     | varchar(254)  |       |         
 ignorersn       | varchar(40)   |       |         
 ignoredate      | timestamp(3)  |       |         
 entproccdval    | varchar(254)  |       |         
 entchgmodfval   | varchar(8000) |       |         
 chgstpdtime     | timestamp(3)  |       |         
 rvusurgeryflag  | bigint        |       |         
 placevalue      | bigint        |       |         
 labonly         | varchar(64)   |       |         
 fhcchg          | varchar(64)   |       |         
 billinglocation | bigint        |       |         
 actingaspcp     | varchar(64)   |       |         
 perfhpid        | varchar(254)  |       |         
 perfhp          | varchar(768)  |       |         
 ordhpid         | varchar(254)  |       |         
 ordhp           | varchar(768)  |       |         
 supvhpid        | varchar(254)  |       |         
 supvhp          | varchar(768)  |       |         
 refhpid         | varchar(254)  |       |         
 refhp           | varchar(768)  |       |         
 primhlthplan    | char(20)      |       |         
 primpayor       | char(20)      |       |         
 sechlthplan     | char(20)      |       |         
 secpayor        | char(20)      |       |         
 glucose         | varchar(1)    |       |         
 lab             | varchar(1)    |       |         
 pathology       | varchar(1)    |       |     

### Below is the MSSQL SQL for the select:
```SQL
USE [SMSPHDSSAWW0]
GO

/****** Object:  View [Customer].[ChargeErrorFullExportableV]    Script Date: 9/20/2023 2:07:52 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create view [Customer].[ChargeErrorFullExportableV]

AS

select Distinct --HPO.RcvOwnerHPOId, SP.RcvOwnerShortName, HPO.ShortName, SP.ShortName,
Err.ChgErrObjID, 
Coalesce(HPO.RcvOwnerHPOId, SP.RcvOwnerHPOId) as RcvOwner,  Coalesce(HPO.ShortName, SP.ShortName) as SvcProv, 
Err.EntSrcName, Err.BatchId,Err.TransTypeMne as TransType, Err.ErrCd,Err.ErrText, Err.FirstErrDTime, 
--Coalesce(Err.EncRpt_EncStrPtName , ER.EncStrPtName, ECD.EncStrPtName, Enc.EncStrPtName) as PtName,
Coalesce(Err.EncRpt_EncMRN, ER.EncMRN, ECD.EncMRN, Enc.EncMRN) as MRN,  
Coalesce(Err.EncId, Err.EncRpt_EncId, Err.EncRpt_IntfcEncId, ER.EncId, Enc.EncId) as EncID,  --, ECD.EncId
Coalesce(Err.ECDNo, Err.EncRpt_ECDNo, ER.ECDNo, ECD.ECDNo, Enc.ECDNo) as ECDNo,
Coalesce(Err.SvcProvSvcId , CF.SvcProvSvcId, SPSM.SvcProvSvcId) as SPSID,
Err.SvcProvIdIssId as SPSIssuer, Svc.SvcId,
Err.SvcIdIssId, Svc.ShortName as SvcName , Err.OvrdSvcName, Err.TransCommonId, Err.EntExtrnId, Err.OrdNo, 
err.ChgStrDTime as ServiceDate, Err.Qty, Err.RxQty, Err.DseQty, Err.NDCQty, Err.ExtPriceAmt, Err.PriceOvrdInd,
Err.EncRpt_EncProvHPOId as EncProv, Err.EncRpt_EncTypeMne as EncType, 
Err.EncRpt_EncStrDTime as EncStrDTime, Err.EncRpt_EncStpDTime as EncStpDTime,  Err.EncRpt_EncStsMne as EncSts, 
Err.EncRpt_EncLocId as EncLoc, Err.EncRpt_LastClinSvcMne as ClinSvc, Err.EncRpt_CreUserId as EncCreUser,
Err.ErrCreDTime, Err.EncRpt_CheckInUserId as CheckInUSer, Err.IgnoreRsnMne as IgnoreRsn, Err.IgnoreDate, 
Err.EntProcCdVal, Err.EntChgModfVal, 
Err.ChgStpDTime,UD.RVUSURGERYFLAG, UD.PlaceValue, 
UD.LABONLY, UD.FHCCHG, UD.BillingLocation, UD.ACTINGASPCP,Err.PerfHlthProfId as PerfHPID, PerfHP.RptName as PerfHP,
Err.OrdHlthProfId as OrdHPID, OrdHP.RptName as OrdHP, Err.SupvHlthProfId as SupvHPID, SupvHP.RptName as SupvHP, 
Err.RefHlthProfId as RefHPID, RefHP.RptName as RefHP, 
HP.ShortName as PrimHlthPlan, PM.ShortName as PrimPayor, HP2.ShortName as SecHlthPlan, PM2.ShortName as SecPayor,
Case
  When Err.SvcProvSvcId in ('1070838','1105858') then 'Y'
  Else 'N'
  End as Glucose,
Case 
  When HPO.Shortname like '%Laboratory%' Then 'Y'
  When Err.SvcProvSvcId in ('1070838','1105858') then 'Y'
  Else 'N'
  End as Lab,
Case 
  When Svc.ShortName like '%Path%' Then 'Y'
  When Svc.ShortName like '%Cyto%' then 'Y'
  When Svc.ShortName like '%G0123%' then 'Y'
  When Svc.ShortName like '%G0124%' then 'Y'
  When Svc.ShortName like '%Thin Prep%' then 'Y'
  Else 'N'
  End as Pathology
from smsdss.ChgErrEncRptV Err
Left Join dbo.ChgErrRptV CE on CE.ChgErrObjID = Err.ChgErrObjID
Left Join smsdss.ChgFctV CF on CF.ChgObjId = Err.ChgObjId
left join smsdss.HPOMstrV HPO on HPO.HPOExtrnId = Err.SvcIdIssId
Left join smsdss.HPOMstrV SP on SP.HPOObjId = Err.SvcProvHPOObjId

left join smsdss.SvcMstrV Svc on Svc.SvcObjid = Err.SvcObjId
LEFT JOIN smsdss.SvcProvSvcMstrV SPSM ON Err.SvcObjId = SPSM.SvcObjId
    AND Err.SvcProvHPOObjId = SPSM.SvcProvHPOObjId 
    AND SPSM.StpDate IS NULL   
left join SvcXpldSet Xpld on Xpld.SvcObjId = Err.SvcObjId 

Left Join smsdss.EncRptV ER on ER.EncObjId = Err.EncObjId
Left Join smsdss.EncRptV Enc on Enc.EncId = Err.EncId
Left Join smsdss.EncRptV ECD on ECD.ECDNo = Err.ECDNo

Left JOIN smsdss.PyrHlthPlanMstrV AS HP  ON HP.PyrHlthPlanObjId = Err.EncRpt_PrimPyrHlthPlanObjId
Left JOIN smsdss.PyrMstrV AS PM ON PM.PyrObjId= Err.EncRpt_PrimPyrObjId
Left Join smsdss.EncPolMemRptV I2 on I2.EncObjId = Err.EncObjId and I2.RankNo = 2
Left Join smsdss.PolMemRptV Pol on Pol.PolMemObjId = I2.PolMemObjId
Left JOIN smsdss.PyrHlthPlanMstrV AS HP2  ON HP2.PyrHlthPlanObjId = Pol.PyrHlthPlanObjId
Left JOIN smsdss.PyrMstrV AS PM2 ON PM2.PyrObjId= Pol.PyrObjId

--left join smsdss.HlthProfMstrV AdmHP on AdmHP.HlthprofObjId = Err.EncRpt_AdmHlthProfObjId
--left join smsdss.HlthProfMstrV AtnHP on AtnHP.HlthprofObjId = Err.EncRpt_AtnHlthProfObjId
left join HlthProfId HPID on HPID.ExtrnId = Err.PerfHlthProfId and HPID.IdDefobjid = 1136051720
left join smsdss.HlthProfMstrV PerfHP on PerfHP.HlthprofObjId = HPID.HlthProfObjId
Left join smsdss.HlthProfMstrV RefHP on RefHP.HlthprofObjId = Err.EncRpt_RefHlthProfObjId

left join HlthProfId HPID2 on HPID2.ExtrnId = Err.OrdHlthProfId and HPID2.IdDefobjid = 1136051720
left join smsdss.HlthProfMstrV OrdHP on OrdHP.HlthprofObjId = HPID2.HlthProfObjId

left join HlthProfId HPID3 on HPID3.ExtrnId = Err.SupvHlthProfId and HPID3.IdDefobjid = 1136051720
left join smsdss.HlthProfMstrV SupvHP on SupvHP.HlthprofObjId = HPID3.HlthProfObjId

Left Join dbo.vw_ChgCommonUserData UD on UD.ChgCommonObjId = Err.ChgCommonObjId
where Err.TransStsMne = 'Err'
--and Err.ErrCd = '20448058'
 and (Left(Err.EncRpt_EncStrPtName,7) <> 'ZZZCERN' or Err.EncRpt_EncStrPtName is Null)
and (CE.TransStsMne = 'Err' or CE.TransStsMne is NULL)
and (PerfHP.RptName Not Like '%zzz%' or PerfHP.RptName is Null)
and (OrdHP.RptName Not Like '%zzz%' or OrdHP.RptName is Null)
and (SupvHP.RptName Not Like '%zzz%' or SupvHP.RptName is Null)
and (RefHP.RptName Not Like '%zzz%' or RefHP.RptName is Null)
--and (Err.ECDNo = '100673505' or Err.EncRpt_ECDNo = '100673505' or ER.ECDNo= '100673505' or ECD.ECDNo = '100673505' or Enc.ECDNo = '100673505')

Union

select Distinct 
Err.ChgErrObjID,
Coalesce(HPO.RcvOwnerHPOId, SP.RcvOwnerHPOId) as RcvOwner,  Coalesce(HPO.ShortName, SP.ShortName) as SvcProv, 
Err.EntSrcName, Err.BatchId,Err.TransTypeMne as TransType, Err.ErrCd,Err.ErrText, Err.FirstErrDTime, 
--Coalesce(CEE.EncRpt_EncStrPtName , ER.EncStrPtName, ECD.EncStrPtName, Enc.EncStrPtName) as PtName,
Coalesce(CEE.EncRpt_EncMRN, ER.EncMRN, ECD.EncMRN, Enc.EncMRN) as MRN,  
Coalesce(Err.EncId, CEE.EncRpt_EncId, CEE.EncRpt_IntfcEncId, ER.EncId, Enc.EncId) as EncID,  --, ECD.EncId
Coalesce(Err.ECDNo, CEE.EncRpt_ECDNo, ER.ECDNo, ECD.ECDNo, Enc.ECDNo) as ECDNo,
Coalesce(Err.SvcProvSvcId , CF.SvcProvSvcId, SPSM.SvcProvSvcId) as SPSID,
Err.SvcProvIdIssId as SPSIssuer, Svc.SvcId,
Err.SvcIdIssId, Svc.ShortName as SvcName , Err.OvrdSvcName, Err.TransCommonId, Err.EntExtrnId, Err.OrdNo, 
err.ChgStrDTime as ServiceDate, Err.Qty, Err.RxQty, Err.DseQty, Err.NDCQty, Err.ExtPriceAmt, Err.PriceOvrdInd,
ER.EncProvHPOId as EncProv, ER.EncTypeMne as EncType, 
ER.EncStrDTime as EncStrDTime, ER.EncStpDTime as EncStpDTime,  ER.EncStsMne as EncSts, 
ER.EncLocId as EncLoc, ER.LastClinSvcMne as ClinSvc, ER.CreUserId as EncCreUser,
Err.ErrCreDTime, ER.CheckInUserId as CheckInUSer, Err.IgnoreRsnMne as IgnoreRsn, Err.IgnoreDate, 
Err.EntProcCdVal, Err.EntChgModfVal, 
Err.ChgStpDTime,UD.RVUSURGERYFLAG, UD.PlaceValue, 
UD.LABONLY, UD.FHCCHG, UD.BillingLocation, UD.ACTINGASPCP,Err.PerfHlthProfId as PerfHPID, PerfHP.RptName as PerfHP,
Err.OrdHlthProfId as OrdHPID, OrdHP.RptName as OrdHP, Err.SupvHlthProfId as SupvHPID, SupvHP.RptName as SupvHP, 
Err.RefHlthProfId as RefHPID, RefHP.RptName as RefHP, 
HP.ShortName as PrimHlthPlan, PM.ShortName as PrimPayor, HP2.ShortName as SecHlthPlan, PM2.ShortName as SecPayor, 
Case
  When Err.SvcProvSvcId in ('1070838','1105858') then 'Y'
  Else 'N'
  End as Glucose,
Case 
  When HPO.Shortname like '%Laboratory%' Then 'Y'
  When Err.SvcProvSvcId in ('1070838','1105858') then 'Y'
  Else 'N'
  End as Lab,
Case 
  When Svc.ShortName like '%Path%' Then 'Y'
  When Svc.ShortName like '%Cyto%' then 'Y'
  When Svc.ShortName like '%G0123%' then 'Y'
  When Svc.ShortName like '%G0124%' then 'Y'
  When Svc.ShortName like '%Thin Prep%' then 'Y'
  Else 'N'
  End as Pathology

from dbo.ChgErrRptV Err
Left Join smsdss.ChgErrEncRptV CEE on CEE.ChgErrObjID = Err.ChgErrObjID
Left Join smsdss.ChgFctV CF on CF.ChgObjId = Err.ChgObjId
left join smsdss.HPOMstrV HPO on HPO.HPOExtrnId = Err.SvcIdIssId
Left join smsdss.HPOMstrV SP on SP.HPOObjId = Err.SvcProvHPOObjId

left join smsdss.SvcMstrV Svc on Svc.SvcObjid = Err.SvcObjId
LEFT JOIN smsdss.SvcProvSvcMstrV SPSM ON Err.SvcObjId = SPSM.SvcObjId
    AND Err.SvcProvHPOObjId = SPSM.SvcProvHPOObjId 
    AND SPSM.StpDate IS NULL   
left join SvcXpldSet Xpld on Xpld.SvcObjId = Err.SvcObjId 

Left Join smsdss.EncRptV ER on ER.EncObjId = Err.EncObjId
Left Join smsdss.EncRptV Enc on Enc.EncId = Err.EncId
Left Join smsdss.EncRptV ECD on ECD.ECDNo = Err.ECDNo

Left JOIN smsdss.PyrHlthPlanMstrV AS HP  ON HP.PyrHlthPlanObjId = ER.PrimPyrHlthPlanObjId
Left JOIN smsdss.PyrMstrV AS PM ON PM.PyrObjId= ER.PrimPyrObjId
Left Join smsdss.EncPolMemRptV I2 on I2.EncObjId = Err.EncObjId and I2.RankNo = 2
Left Join smsdss.PolMemRptV Pol on Pol.PolMemObjId = I2.PolMemObjId
Left JOIN smsdss.PyrHlthPlanMstrV AS HP2  ON HP2.PyrHlthPlanObjId = Pol.PyrHlthPlanObjId
Left JOIN smsdss.PyrMstrV AS PM2 ON PM2.PyrObjId= Pol.PyrObjId

--left join smsdss.HlthProfMstrV AdmHP on AdmHP.HlthprofObjId = Err.EncRpt_AdmHlthProfObjId
--left join smsdss.HlthProfMstrV AtnHP on AtnHP.HlthprofObjId = Err.EncRpt_AtnHlthProfObjId
left join HlthProfId HPID on HPID.ExtrnId = Err.PerfHlthProfId and HPID.IdDefobjid = 1136051720
left join smsdss.HlthProfMstrV PerfHP on PerfHP.HlthprofObjId = HPID.HlthProfObjId
Left join smsdss.HlthProfMstrV RefHP on RefHP.HlthprofObjId = ER.RefHlthProfObjId

left join HlthProfId HPID2 on HPID2.ExtrnId = Err.OrdHlthProfId and HPID2.IdDefobjid = 1136051720
left join smsdss.HlthProfMstrV OrdHP on OrdHP.HlthprofObjId = HPID2.HlthProfObjId

left join HlthProfId HPID3 on HPID3.ExtrnId = Err.SupvHlthProfId and HPID3.IdDefobjid = 1136051720
left join smsdss.HlthProfMstrV SupvHP on SupvHP.HlthprofObjId = HPID3.HlthProfObjId

Left Join dbo.vw_ChgCommonUserData UD on UD.ChgCommonObjId = Err.ChgCommonObjId
where Err.TransStsMne = 'Err'
 and (Left(ER.EncStrPtName,7) <> 'ZZZCERN' or ER.EncStrPtName is Null)
 and (PerfHP.RptName Not Like '%zzz%' or PerfHP.RptName is Null)
and (OrdHP.RptName Not Like '%zzz%' or OrdHP.RptName is Null)
and (SupvHP.RptName Not Like '%zzz%' or SupvHP.RptName is Null)
and (RefHP.RptName Not Like '%zzz%' or RefHP.RptName is Null)


GO
```