CREATE OR REPLACE PROCEDURE `gcp-bia-mle-dev.UPSC_DITK.RTSExpCC_sp_01_Excp_forFraudRerouteRTSAddrScan`()
BEGIN


/*************************************************************************************************************************
     STEP 1: Daily refresh 
BQ_JOBS   --> NEED TO BE ADDED IN CTS DAILY 
**************************************************************************************************************************/

DECLARE     StartDateTime      TIMESTAMP;
    SET    StartDateTime    =  '2022-01-01'  ;
  --  SELECT  StartDateTime ;


  ----- CALL `gcp-bia-mle-dev.UPSC_DITK.RTSExpCC_sp_01_Excp_forFraudRerouteRTSAddrScan`() -- created all dev tables


/*****************************************************************************************************************************
          STEP 1: Creation of the FraudScan Table used to identify packages that received a fraud risk/hold exception scan  
NOTE : DATA UNTIL YESTERDAY 
******************************************************************************************************************************/



CREATE OR REPLACE TABLE  `gcp-bia-mle-dev.UPSC_DITK.FactExcpFraudScan` 
PARTITION BY
    MsgEventLocalTs_Date
CLUSTER BY
    TrackingNumber
AS 
---------------------------------------------------------------------------------------------
WITH CTE_GetFraudExcpFromPkgScanDetails 
AS (
  SELECT DISTINCT 
            TRIM(GIO.TrackingNumber)    as TrackingNumber,
            GIO.Shipper_AcCnyCd,
            GIO.Shipper_AcNr,
            RIGHT(LEFT(TRIM(GIO.TrackingNumber),8),6) as AC_NR,
            GIO.MsgEventTs,
            GIO.MsgEventLocalTs,
            DATE(GIO.MsgEventLocalTs)        as MsgEventLocalTs_Date,
            GIO.FacMneNa,
            GIO.OgzNr,
            GIO.PkgLocRlCd,
            GIO.HeatCategory,
            GIO.ExcpUserID,
            GIO.ExcpReason,
            MAP.DelivLocDesc   as ExcpReasonDesc,
            GIO.ExcpResol,
            MAP2.DelivLocDesc   as ExcpResolDesc,
            GIO.ExcpStatus,
            ROW_NUMBER() OVER (PARTITION BY TrackingNumber, TIMESTAMP_TRUNC(MsgEventLocalTs,DAY) ORDER BY MsgEventLocalTs DESC) as FraudFlag
    -----------------------------------------------------------------------------------    
    FROM `gcp-bia-mle-prod.srcUPSHEAT.PackageScanDetails` GIO
    LEFT JOIN `gcp-bia-mle-prod.srcDimensionalTables.DimUPSPackageLocationStatus` MAP
              ON   GIO.ExcpReason = MAP.DelivLocCd
    LEFT JOIN `gcp-bia-mle-prod.srcDimensionalTables.DimUPSPackageLocationStatus` MAP2
            ON GIO.ExcpResol     = MAP2.DelivLocCd
    ---------------------------------------------------------------------------------------------------------------------
          WHERE     GIO.MsgEventTs      >=    StartDateTime
     
      ----------------------------------------------------------------------------------------------------
            AND GIO.ExcpReason       IN      ('R0','Y8','LU','4O','FP','79','TC') 
            AND GIO.ExcpResol       NOT IN    ('TL','TM')
     ------------------------------------------------------------------------------------------------------------------------
)
  SELECT *  FROM CTE_GetFraudExcpFromPkgScanDetails       WHERE      FraudFlag = 1      ;
----------------------------------------------------------------------------------------------------------------






/************************************************************************************************************************
                STEP 2 : Creation of the RerouteScan Table used to identify packages that received a reroute exception scan  
*************************************************************************************************************************/



CREATE OR REPLACE TABLE `gcp-bia-mle-dev.UPSC_DITK.FactExcpRerouteScan` 
PARTITION BY
    MsgEventLocalTs_Date
CLUSTER BY
    TrackingNumber
AS 
---------------------------------------------------------------------------------------------
WITH CTE_GetRerouteExcpFromPkgScanDetails 
AS (
  SELECT DISTINCT 
            TRIM(GIO.TrackingNumber)    as TrackingNumber,
            GIO.Shipper_AcCnyCd,
            GIO.Shipper_AcNr,
            RIGHT(LEFT(TRIM(GIO.TrackingNumber),8),6) as AC_NR,
            GIO.MsgEventTs,
            GIO.MsgEventLocalTs,
            DATE(GIO.MsgEventLocalTs)        as MsgEventLocalTs_Date,
            GIO.FacMneNa,
            GIO.OgzNr,
            GIO.PkgLocRlCd,
            GIO.HeatCategory,
            GIO.ExcpUserID,
            GIO.ExcpReason,
            MAP.DelivLocDesc   as ExcpReasonDesc,
            GIO.ExcpResol,
            MAP2.DelivLocDesc   as ExcpResolDesc,
            GIO.ExcpStatus,
        ROW_NUMBER() OVER (PARTITION BY TrackingNumber, TIMESTAMP_TRUNC(MsgEventLocalTs,DAY) ORDER BY MsgEventLocalTs DESC) as RerouteFlag
   --------------------------------------------------  --------------------------------------------------  --------------------------------------------------       
    FROM `gcp-bia-mle-prod.srcUPSHEAT.PackageScanDetails` GIO
    LEFT JOIN `gcp-bia-mle-prod.srcDimensionalTables.DimUPSPackageLocationStatus` MAP
          ON   GIO.ExcpReason = MAP.DelivLocCd
    LEFT JOIN `gcp-bia-mle-prod.srcDimensionalTables.DimUPSPackageLocationStatus` MAP2
          ON   GIO.ExcpResol = MAP2.DelivLocCd
    --------------------------------------------------------------------------------------------------------------
        WHERE     GIO.MsgEventTs      >=    StartDateTime   
         AND     ExcpResol      IN ('3R','O7','5S','97')
  -----------------------------------------------------------------------------------------------------------
    )    
    SELECT *  FROM CTE_GetRerouteExcpFromPkgScanDetails       WHERE      RerouteFlag = 1  ;





/********************************************************************************************************************
            STEP 3: Creation of the RTSTable used to identify packages that received a Return to Sender exception scan  
*********************************************************************************************************************/


CREATE OR REPLACE TABLE  `gcp-bia-mle-dev.UPSC_DITK.FactExcpRTSScan` 
PARTITION BY
    MsgEventLocalTs_Date
CLUSTER BY
    TrackingNumber
AS 
---------------------------------------------------------------------------------------------
WITH CTE_GetRTSExcpFromPkgScanDetails 
AS (
   SELECT DISTINCT 
            TRIM(GIO.TrackingNumber)    as TrackingNumber,
            GIO.Shipper_AcCnyCd,
            GIO.Shipper_AcNr,
            RIGHT(LEFT(TRIM(GIO.TrackingNumber),8),6) as AC_NR,
            GIO.MsgEventTs,
            GIO.MsgEventLocalTs,
            DATE(GIO.MsgEventLocalTs)        as MsgEventLocalTs_Date,
            GIO.FacMneNa,
            GIO.OgzNr,
            GIO.PkgLocRlCd,
            GIO.HeatCategory,
            GIO.ExcpUserID,
            GIO.ExcpReason,
            MAP.DelivLocDesc   as ExcpReasonDesc,
            GIO.ExcpResol,
            MAP2.DelivLocDesc   as ExcpResolDesc,
            GIO.ExcpStatus,
        ROW_NUMBER() OVER (PARTITION BY TrackingNumber, TIMESTAMP_TRUNC(MsgEventLocalTs,DAY) ORDER BY MsgEventLocalTs DESC) as RTSFlag
  -----------------------------------------------------------------------------------  
  FROM `gcp-bia-mle-prod.srcUPSHEAT.PackageScanDetails` GIO
    LEFT JOIN `gcp-bia-mle-prod.srcDimensionalTables.DimUPSPackageLocationStatus` MAP
           ON GIO.ExcpReason = MAP.DelivLocCd
    LEFT JOIN `gcp-bia-mle-prod.srcDimensionalTables.DimUPSPackageLocationStatus` MAP2
          ON GIO.ExcpResol = MAP2.DelivLocCd
    -------------------------------------------------------------------------------------------------------------------
       WHERE     GIO.MsgEventTs      >=    StartDateTime    
       AND       ExcpResol       IN   ('IA','XU','27','A1','4S','65','24','GR','UF','O4','I1','CS','YJ','YK','FF','H8','RV','3N','4R','53','6C','8H','90','DL','TU','MI','7H','VO')
)
  SELECT *  FROM CTE_GetRTSExcpFromPkgScanDetails       WHERE      RTSFlag = 1  ;




/***************************************************************************************************************************************
     STEP:4 Creation of the Address Correction Table used to identify packages that received a address correction exception scan  
*****************************************************************************************************************************************/




CREATE OR REPLACE TABLE  `gcp-bia-mle-dev.UPSC_DITK.FactExcpAddCorScan` 
PARTITION BY
    MsgEventLocalTs_Date
CLUSTER BY
    TrackingNumber
AS 
---------------------------------------------------------------------------------------------
WITH CTE_GetAddCorExcpFromPkgScanDetails 
AS (
    SELECT DISTINCT 
            TRIM(GIO.TrackingNumber)    as TrackingNumber,
            GIO.Shipper_AcCnyCd,
            GIO.Shipper_AcNr,
            RIGHT(LEFT(TRIM(GIO.TrackingNumber),8),6) as AC_NR,
            GIO.MsgEventTs,
            GIO.MsgEventLocalTs,
            DATE(GIO.MsgEventLocalTs)        as MsgEventLocalTs_Date,
            GIO.FacMneNa,
            GIO.OgzNr,
            GIO.PkgLocRlCd,
            GIO.HeatCategory,
            GIO.ExcpUserID,
            GIO.ExcpReason,
            MAP.DelivLocDesc   as ExcpReasonDesc,
            GIO.ExcpResol,
            MAP2.DelivLocDesc   as ExcpResolDesc,
            GIO.ExcpStatus,
        ROW_NUMBER() OVER (PARTITION BY TrackingNumber, TIMESTAMP_TRUNC(MsgEventLocalTs,DAY) ORDER BY MsgEventLocalTs DESC) as AddCorFlag
  FROM `gcp-bia-mle-prod.srcUPSHEAT.PackageScanDetails` GIO
    LEFT JOIN `gcp-bia-mle-prod.srcDimensionalTables.DimUPSPackageLocationStatus` MAP
         ON GIO.ExcpReason = MAP.DelivLocCd
    LEFT JOIN `gcp-bia-mle-prod.srcDimensionalTables.DimUPSPackageLocationStatus` MAP2
         ON GIO.ExcpResol = MAP2.DelivLocCd
    -----------------------------------------------------------------------------------------------------
            WHERE     GIO.MsgEventTs      >=    StartDateTime   
             AND     ExcpResol IN ('9I','AH','6G')
)
--------------------------------------------------------------------------------------------
  SELECT *  FROM CTE_GetAddCorExcpFromPkgScanDetails       WHERE      AddCorFlag = 1  ;
--------------------------------------------------------------------------------------------





/***********************************************************************************************************************************************

    SELECT distinct  MsgEventTs from      `gcp-bia-mle-prod.UPSC_DITK.FactExcpFraudScan`   where  date(MsgEventTs)   =  '2024-04-24'  order by 1  -- 3895   2024-04-24 00:00:10 UTC  -- 2024-04-24 23:50:06 UTC
    SELECT distinct  MsgEventTs from      `gcp-bia-mle-prod.UPSC_DITK.FactExcpRerouteScan` where  date(MsgEventTs)   =  '2024-04-24'  order by 1  -- 41530  2024-04-24 00:00:14 UTC  -- 2024-04-24 23:59:59 UTC
    SELECT distinct  MsgEventTs from      `gcp-bia-mle-prod.UPSC_DITK.FactExcpRTSScan`     where  date(MsgEventTs)   =  '2024-04-24'  order by 1  --  48634 2024-04-24 23:59:59 UTC
    SELECT distinct  MsgEventTs from      `gcp-bia-mle-prod.UPSC_DITK.FactExcpAddCorScan`  where  date(MsgEventTs)   =  '2024-04-24'  order by 1  --  48634  2024-04-24 23:59:59 UTC


******************************************************************************************************************************************************************************/













END;
