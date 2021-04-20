# DISCHARGEDB



## Insert SAS-table (sas7bdat) into MySQL-tabel

- Set size of row
```
SET @@global.sql_mode= '';
```
- Insert table: 
```
sas2db --db mysql+pymysql://root:123@localhost:3306/dischargedb3?charset=utf8mb4 H:/cloud/cloud_data/Projects/DISCHARGEDB/data/tmp/ecrf/v_b02_ica_upload.sas7bdat
```

Flow:
- Create DISCHARGEDB
- Insert SAS tables
- Insert DICOM data
- INSERT additional tables



## Backup

- 



## GitHub

- 

## DOCU

- Create documentation



## TODO

- Auto increment primary key  (https://stackoverflow.com/questions/5402949/mysql-cant-make-column-auto-increment)
- Download images from agmednet and upload in database
- Create log strategy