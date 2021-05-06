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





## TODO

- Auto increment primary key  (https://stackoverflow.com/questions/5402949/mysql-cant-make-column-auto-increment)
- Download images from agmednet and upload in database
- Create prolem table



## Installation (Windows)

- Install XAMPP  https://www.apachefriends.org/de/download.html

- Change config files for apache server (see doc/config/config.inc.php)

- Change config files for MySQLserver (see doc/config/my.ini)

  - Set IP Address according to charite IP e.g.: 

- Disable Firewall to test connection

- Option: Install MySQl Workbench https://www.mysql.com/de/products/workbench/

- Enable remote access: 

  - Edit the *apache/conf/extra/httpd-xampp.conf* file in your XAMPP installation directory (usually, *C:\xampp*).

  - Within this file, find the block below:

    ```
    <Directory "/xampp/phpMyAdmin">
      AllowOverride AuthConfig
      Require local
      ...
    ```

    Update this block and replace *Require local* with *Require all granted*, so that it looks like this:

    ```
    <Directory "/xampp/phpMyAdmin">
      AllowOverride AuthConfig
      Require all granted
      ...
    ```

  - Save the file and restart the Apache server using the XAMPP control panel.

- Replace MariaDB by MySQL by following this instructions (https://gist.github.com/alfhh/92ae13b2592ac654cfe924319259136f)
- config.php set $cfg['Servers'][$i]['AllowNoPassword'] = TRUE; to allow no password
- To avoit the phpmyadmin error "The secret passphrase in configuration (blowfish_secret) is too short" follow instuctions here (https://www.mysysadmintips.com/linux/servers/779-the-secret-passphrase-in-configuration-blowfish-secret-is-too-short)
- To avoid error "The phpMyAdmin configuration storage is not completely configured, some extended features have been deactivated" follw instructions here (https://stackoverflow.com/questions/33824224/phpmyadmin-configuration-storage-is-not-completely-configured)

- Add user:

  - Login as root into database with: mysql -u root -p
  - Create user with: CREATE 'USER'@'HOST-IP' IDENTIFIED BY 'PASSWORD'; (e.g. CREATE USER  cw@10.39.64.141 IDENTIFIED BY '789';)
  - Give access rights: GRANT ALL ON *.* TO 'USER@'USER-IP';flush privileges; (e.g. GRANT ALL ON *.* TO cw@10.39.64.117;flush privileges;)

IP: 169.254.31.125

- Access: 
  - Over phpMyAdmin type in the browser (Firefox, Chrome are tested): http://141.42.31.62 
  -  Use MySQl Workbench https://www.mysql.com/de/products/workbench/

## Backup

- 



## GitHub

- 

## DOCU

- Create documentation

