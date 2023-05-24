REPLACE PROCEDURE DL_ESI_Prod.sp_CPT_RVU_CMS()

/*Purpose: Load data from staging table to prod table, leveraging SCD type 2. There are three considerations: */
/*(1) When a new year begins, add all records from stg table*/
/*(2) Throughout a specific year, if any new CPT codes are captured by the staging table, then add a new record into the prod table based on data in stg table */
/*(3) Throughout a specific year, if any change in RVU value for an existing CPT code is identified, then firstly add a new record for the new value to the prod table; secondly update the end_date for the old value to be the effective date of the new value */

/*join on Yr, CPT and end_date is null in order to get the most recent RVU value throughout a year*/

/*Author: XIONGN*/
/*v0: 4/7/23*/

BEGIN
	
/*Step 1: Insert new records into prod table from stg table if 
 * (1) a new year begins; 
 * or (2) throughout a specific year, new CPT codes are captured by the staging table; 
 * or (3) throughout a specific year, change in RVU value for an existing CPT code is identified */		

	INSERT INTO DL_ESI_PROD.CPT_RVU_CMS
	SELECT 
		YEAR(CAST(effective_date AS TIMESTAMP)) AS Yr,
		CPT_HCPCS_Code AS CPT,
        CAST(RVU AS DECIMAL(38,6)) AS Unit_RVU,
        CAST(Effective_Date AS TIMESTAMP) AS Start_Date,  /*use effective date as start date*/
        NULL AS End_Date,     /*use null as end_date */
        CAST(Insert_Dttm AS TIMESTAMP) AS Insert_Dttm
	FROM DL_ESI_Staging.stg_CPT_RVU_CMS stg
	WHERE NOT EXISTS 
		(
		SELECT 
			Yr,
      		CPT,
      		Unit_RVU,
      		Start_Date,
      		End_Date,
      		insert_dttm
		FROM DL_ESI_PROD.CPT_RVU_CMS prod
		WHERE prod.yr = YEAR(CAST(stg.effective_date AS TIMESTAMP))
			AND prod.cpt = stg.CPT_HCPCS_Code
			AND prod.unit_rvu = CAST(stg.RVU AS DECIMAL(38,6))   /*if the Yr, CPT and RVU combination differs between prod table and stg table, then that means a new row should be inserted*/
		);
		
	COLLECT STATISTICS COLUMN (CPT, Start_Date, End_Date) on DL_ESI_PROD.CPT_RVU_CMS;
	
	
/*Step 2: Update the end_date for the old value to be the effective date of the new value*/	

	UPDATE prod
	FROM DL_ESI_PROD.CPT_RVU_CMS prod, 
		DL_ESI_Staging.stg_CPT_RVU_CMS stg
	SET end_date = CAST(stg.effective_date AS TIMESTAMP)
	WHERE prod.Yr = YEAR(CAST(stg.effective_date AS TIMESTAMP))
		AND prod.cpt = stg.CPT_HCPCS_Code
		AND prod.unit_rvu <> CAST(stg.rvu AS DECIMAL(38,6))
		AND prod.end_date is null;
	
	COLLECT STATISTICS COLUMN (CPT, Start_Date, End_Date) on DL_ESI_PROD.CPT_RVU_CMS;
	
END;