CREATE PROCEDURE `patient_status` ()
BEGIN
SELECT 
	PatientID,
	COUNT(IF(CLASS = 'CTA', 1, NULL)) 'num_cta',     
	COUNT(IF(CLASS = 'CACS', 1, NULL)) 'num_cacs',     
	COUNT(IF(CLASS = 'NCS_CACS', 1, NULL)) 'num_ncs_cta',     
	COUNT(IF(CLASS = 'NCS_CTA', 1, NULL)) 'num_ncs_cacs'     
	FROM discharge_master_01092020_master_01092020
    GROUP BY PatientID;
END
