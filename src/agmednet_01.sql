CREATE TABLE `agmednet_01` (
  `idagmednet_01` int NOT NULL AUTO_INCREMENT,
  `trial` varchar(45) DEFAULT NULL,
  `study_instance_uid` varchar(255) DEFAULT NULL,
  `exam_transfer_date` datetime DEFAULT NULL,
  `exam_date` date DEFAULT NULL,
  `transmission_status` varchar(45) DEFAULT NULL,
  `site` varchar(45) DEFAULT NULL,
  `subject` varchar(45) DEFAULT NULL,
  `patient_name` varchar(45) DEFAULT NULL,
  `patient_id` varchar(45) DEFAULT NULL,
  `timepoint` int DEFAULT NULL,
  `sender_email` varchar(100) DEFAULT NULL,
  `modality` varchar(45) DEFAULT NULL,
  `accession_number` varchar(45) DEFAULT NULL,
  `association_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`idagmednet_01`)
) ENGINE=InnoDB AUTO_INCREMENT=15635 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci