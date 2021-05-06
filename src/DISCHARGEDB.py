# -*- coding: utf-8 -*-
"""
Created on Fri Apr 16 15:46:23 2021

@author: bernifoellmer
"""

import os, sys
import ntpath
from glob import glob
import pandas as pd
from sas7bdat import SAS7BDAT
#import mysql.connector
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from settings import initSettings, saveSettings, loadSettings, fillSettingsTags
import xlrd
from helper import splitFilePath
from agmednet import mednet_down
import zipfile
import logger
import logging
from datetime import datetime
from tqdm import tqdm
DBLogger = logging.getLogger('DISCHARGEDB')

class DISCHARGEDB:
    host = ''
    port = ''
    user = ''
    password = ''
    database = ''
    db = None
    
    def __init__(self, host="127.0.0.1", port='3306', user="root", password="123", database='dischargedb'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.db = None
        
    def download_images(self, settings):
        mednet_down(settings['fp_download'], settings['fp_images'], settings['fip_report'])
    
    def update_images(self, settings):
        fp_images = settings['fp_images']
        fp_download = settings['fp_download']
        files = glob(fp_download + '/*.zip')
        for file in files:
            print('Extract: ' + file)
            DBLogger.debug('Extract: ' + file)
            with zipfile.ZipFile(file,"r") as zip_ref:
                zip_ref.extractall(fp_images)
                
    def resetAutoIncrement(self, tablename='agmednet_01', idname='idagmednet_01'):
        self.connectSQL()
        with self.engine.connect() as con:
            command = "SET SQL_SAFE_UPDATES=0; SET  @num := 0; UPDATE " + tablename + " SET " + idname + " = @num := (@num+1); ALTER TABLE " + tablename + " AUTO_INCREMENT =1; SET SQL_SAFE_UPDATES=1;"
            rs = con.execute(command)
        return rs

    def deleteRows(self, tablename='agmednet_01'):
        self.connectSQL()
        command = "SET SQL_SAFE_UPDATES=0; DELETE FROM " + tablename + "; SET SQL_SAFE_UPDATES=1;"
        self.db.cursor().execute(command)  
        
    def truncateTable(self, tablename='agmednet_02'):
        self.connectSQL()
        with self.engine.connect() as con:
            command = "TRUNCATE TABLE %s;" % tablename
            rs = con.execute(command)
        return rs

    def update_agmednet_01(self, settings):
        # Update AG_Mednet_Report_01
        self.connectSQL()
        # Truncate tabel
        # Read AG_Mednet_Report_01 excel file
        df = pd.read_excel(settings['AG_Mednet_Report_01'])
        df_sub = df[0:]
        df_sub = df_sub.where(pd.notnull(df_sub), None)
        # Define keys and values
        keys = ['trial', 'study_instance_uid', 'exam_transfer_date', 'exam_date', 'transmission_status', 'site', 'subject', 
                'patient_name', 'patient_id', 'timepoint','sender_email', 'modality', 'accession_number', 'association_id']
        keys_str = '(' + ', '.join(keys) + ')'
        values = ['%s' for i in range(len(keys))]
        values_str = '(' + ', '.join(values) + ')'
        # Define command
        command="REPLACE INTO agmednet_01 " + keys_str + " VALUES " + values_str
        # Update tabel
        pbar = tqdm(total=len(df_sub))
        pbar.set_description("Update AG_Mednet_Report_01")
        for index, row in df_sub.iterrows():
            pbar.update(1)
            vallist = list(row)
            # Update timestamp
            vallist[2] = datetime.strptime(vallist[2], '%d-%b-%Y %I:%M %p')
            vallist[3] = datetime.strptime(vallist[3], '%d-%b-%Y')
            val = tuple(vallist)
            conn = self.engine.connect()
            trans = conn.begin()
            conn.execute(command, val)
            trans.commit()
            conn.close()
        pbar.close()
        # Reset auto increment
        self.resetAutoIncrement(tablename='agmednet_01', idname='idagmednet_01')
        
    def update_agmednet_02(self, settings):
        # Update AG_Mednet_Report_02
        self.connectSQL()
        # Read AG_Mednet_Report_01 excel file
        df = pd.read_excel(settings['AG_Mednet_Report_02'])
        df_sub = df[0:]
        df_sub = df_sub.where(pd.notnull(df_sub), None)
        # Define keys and values
        keys = ['trial', 'transmittal_form_name', 'study_instance_uid', 'exam_transfer_date', 'exam_date', 'patient_id', 
                'dicom_study_date', 'acquisition_start_time', 'non_dicom_study_date', 'acquisition_end_time', 'examination',
                'stress_tests_other', 'lung_nodule_follow_up_other', 'other_tests_other','site_contact_submitter', 
                'date_of_submission', 'dicomscandate', 'dicomimagedata', 'dicomacqstart', 'hidecct', 'hideica', 'hidecacs',
                'hidenonmain','hidedicom', 'hidenotdicom', 'additional_comments']
        keys_str = '(' + ', '.join(keys) + ')'
        values = ['%s' for i in range(len(keys))]
        values_str = '(' + ', '.join(values) + ')'
        # Define command
        command="REPLACE INTO agmednet_02 " + keys_str + " VALUES " + values_str
        # Update tabel
        pbar = tqdm(total=len(df_sub))
        pbar.set_description("Update AG_Mednet_Report_02")
        for index, row in df_sub.iterrows():
            pbar.update(1)
            vallist = list(row)
            # Update 
            if vallist[3] is not None:
                vallist[3] = datetime.strptime(vallist[3], '%d-%b-%Y %I:%M %p')
            if vallist[4] is not None:
                vallist[4] = datetime.strptime(vallist[4], '%d-%b-%Y')
            if vallist[6] is not None:
                vallist[6] = datetime.strptime(vallist[6], '%d/%b/%Y')
            if vallist[7] is not None:
                vallist[7] = datetime.strptime(vallist[7], '%H:%M')
            if vallist[8] is not None:
                vallist[8] = datetime.strptime(vallist[8], '%d/%b/%Y')
            if vallist[9] is not None:
                if vallist[9]=='..' or vallist[9]=='/':
                    vallist[9]=None
                else:
                    vallist[9] = vallist[9].replace('-',':')
                    vallist[9] = vallist[9].replace('.',':')
                    vallist[9] = datetime.strptime(vallist[9], '%H:%M')
            if vallist[15] is not None:
                vallist[15] = datetime.strptime(vallist[15], '%d/%b/%Y')
            val = tuple(vallist)
            conn = self.engine.connect()
            trans = conn.begin()
            conn.execute(command, val)
            trans.commit()
            conn.close()

        pbar.close()
        
        # Reset auto increment
        self.resetAutoIncrement(tablename='agmednet_02', idname='idagmednet_02')
        
    def update_dicom(self, settings):
        
        
        df = pd.read_excel('H:/cloud/cloud_data/Projects/DISCHARGEDB/code/data/tables/xlsx/discharge_dicom_01092020.xlsx')
        df.drop('Unnamed: 0', axis=1, inplace=True)
        df_sub = df[0:300]
        df_sub = df_sub.where(pd.notnull(df_sub), None)
        
        self = db
        #self.insertSQL(command="INSERT INTO dicom (site, patientid, count) VALUES (%s, %s, %s)", values=('123', '123', 4))
        keys = ['site', 'patientid', 'studyinstanceuid', 'seriesinstanceuid', 'acquisitiondate', 'seriesnumber', 'count', 'seriesdescription',
                'modality', 'acquisitiontime', 'numberofframes', 'rowsdicom', 'columnsdicom', 'instancenumber', 'patientsex', 'patientage', 'protocolname',
                'contrastbolusagent', 'imagecomments', 'pixelspacing', 'slicethickness ', 'filtertype', 'convolutionkernel', 'reconstructiondiameter', 
                'requestedproceduredescription', 'contrastbolusstarttime','nominalpercentageofcardiacphase', 'cardiacrrintervalspecified',
                'studydate', 'slicespacing']
        keys_str = '(' + ', '.join(keys) + ')'
        values = ['%s' for i in range(len(keys))]
        values_str = '(' + ', '.join(values) + ')'
        command="INSERT IGNORE INTO dicom " + keys_str + " VALUES " + values_str

        for index, row in df_sub.iterrows():
            vallist = list(row)
            vallist[9] = datetime.now(tz=None)
            vallist[25] = datetime.now(tz=None)
            val = tuple(vallist)
            try:
                cursor = self.db.cursor()
                cursor.execute(command, val)
                self.db.commit()
                print(cursor.rowcount, "record inserted.")
            except:
                print('Row already exist.')
        
    def createDB(self):
        command_create = "CREATE DATABASE if not exists " + self.database
        self.db.cursor().execute(command_create)
        # Init database
        command = "SET @@global.sql_mode= '';"
        self.db.cursor().execute(command)
        
    def initDB(self, settings):

        # Upload xlsx files
        files = glob(settings['folderpath_xlsx'] + '/*.xlsx')
        for file in files:
            print('Upload: ' + file)
            self.xlxsTosql(database=self.database, 
                            host=self.host, 
                            port=self.port, 
                            user=self.user, 
                            password=self.password, 
                            fip_xlsx=file)
        # Upload sas files
        files = glob(settings['folderpath_sas'] + '/*.sas7bdat')
        for file in files:
            print('Upload: ' + file)
            self.sas7bdatTosql(database=self.database, 
                            ip=self.host, 
                            port=self.port, 
                            un=self.user, 
                            pw=self.password, 
                            fip_sas=file)          
        
    def connectSQL(self):
        # self.db = mysql.connector.connect(
        #   host=self.host,
        #   user=self.user,
        #   password=self.password,
        #   database=self.database
        # )
        mysql_path = 'mysql://' + self.user + ':' + self.password + '@' + self.host + '/'+ self.database + '?charset=utf8'
        self.engine = create_engine(mysql_path)

    def insertSQL(self, command="INSERT INTO dicom (site, patientid) VALUES (%s, %s)", values=('John', 'Highway 21')):
        try:
            cursor = self.db.cursor()
            cursor.execute(command, values)
            self.db.commit()
            print(cursor.rowcount, "record inserted.")
        except:
            print('Connection to mysql database failed.')
            
    def selectSQL(self, command='SELECT * FROM dischargedb3.site;'):
        try:
            self.connectSQL()
            cursor = self.db.cursor()
            cursor.execute(command)
            result = cursor.fetchall()
            
            return result
        except:
            print('Connection to mysql database failed.')
            
    def closeSQL(self):
        self.db.close()

    def sas7bdatTosql(self, database='dischargedb3', ip='localhost', port='3306', un='root', pw='123', fip_sas='H:/cloud/cloud_data/Projects/DISCHARGEDB/data/tmp/ecrf/v_f01_physex_vs.sas7bdat'):
        try:
            _, filename, _ = splitFilePath(fip_sas)
            command = 'sas2db --db mysql+pymysql://' + un + ':' + pw + '@' + ip + ':' + port + '/' + database + '?charset=utf8mb4 ' + ' --table ' + filename + ' ' + fip_sas
            #print('command123', command)
            r = os.system(command)
            # Set primary key
            #self.executeSQL('ALTER TABLE ' + filename + ' ADD PRIMARY KEY (index)')

        except:
            print('Upload sas table in database failed.')
        return r
            
    def xlxsTosql(self, database='dischargedb3', host='localhost', port='3306', user='root', password='123', fip_xlsx='H:/cloud/cloud_data/Projects/DISCHARGEDB/data/tables/xlsx/discharge_master_01092020.xlsx'):
        try:
            xls = xlrd.open_workbook(fip_xlsx)
            sheet_names = xls.sheet_names()
            for sheet_name in sheet_names:
                df = pd.read_excel(fip_xlsx, sheet_name=sheet_name)
                df.columns = df.columns.astype(str)
                mysql_path = 'mysql://' + user + ':' + password + '@' + host + '/'+ database + '?charset=utf8'
                engine = create_engine(mysql_path)
                _, filename, _ = splitFilePath(fip_xlsx)
                table = (filename + '_' + sheet_name).lower()
                df.to_sql(table, engine, index=False, if_exists='replace')
        except:
            print('Upload xlxs table in database failed.')
        return 0

            
    def executeScript(self, fip_script='H:/cloud/cloud_data/Projects/DISCHARGEDB/src/scripts/set_primary_key.sql', replace=('','')):
        mysql_path = 'mysql://' + self.user + ':' + self.password + '@' + self.host + '/'+ self.database + '?charset=utf8'
        engine = create_engine(mysql_path)
        file = open(fip_script)
        escaped_sql = sqlalchemy.text(file.read())
        escaped_sql.text = escaped_sql.text.replace(replace[0], replace[1])
        engine.execute(escaped_sql)
        
    def getTable(self, tablename='agmednet_01'):
        self.connectSQL()
        df = pd.read_sql("SELECT * FROM " + tablename, self.engine)
        return df
        
#if __name__=='__main__':   
def main():
    
    # Load settings
    filepath_settings = 'H:/cloud/cloud_data/Projects/DISCHARGEDB/code/data/settings.json'
    settings=initSettings()
    saveSettings(settings, filepath_settings)
    settings = fillSettingsTags(loadSettings(filepath_settings))
        
    #### Downlaod new images from ag mednet #####
    #discharge = DISCHARGEDB(database=settings['database'])
    #discharge.download_images(settings)
    #discharge.update_images(settings)
    #discharge.update_dicom(settings)
    
    ### Update agmednet reports ###
    discharge = DISCHARGEDB(host="127.0.0.1", port='3306', user="root", password="123", database=settings['database'])
    discharge.update_agmednet_01(settings)
    discharge.update_agmednet_02(settings)
    
    discharge = DISCHARGEDB(host="127.0.0.1", port='3306', user="root", password="123", database=settings['database'])
    rs = discharge.truncateTable('agmednet_02')
    discharge.update_agmednet_02(settings)
    df = discharge.getTable('agmednet_02')
    
    df = discharge.getTable('agmednet_01')
    
    #### Execute sript #####
    discharge = DISCHARGEDB(host="127.0.0.1", port='3306', user="root", password="123", database=settings['database'])
    table = discharge.getTable('agmednet_01')
    discharge.truncateTable('agmednet_01')
    discharge.truncateTable('agmednet_02')
    discharge.connectSQL()
    
    table = discharge.getTable('agmednet_01')
    table = discharge.getTable('agmednet_01')

    
    self=db
    mysql_path = 'mysql://' + self.user + ':' + self.password + '@' + self.host + '/'+ self.database + '?charset=utf8'
    sqlEngine = create_engine(mysql_path)
    df = pd.read_sql("SELECT * FROM agmednet_01", sqlEngine)
    
    ### Reset autoincrement
    db = DISCHARGEDB(host="127.0.0.1", port='3306', user="root", password="123", database=settings['database'])
    db.connectSQL()
    db.resetAutoIncrement()
    
    #db.createDB()
    db.initDB(settings)
    db.executeScript(fip_script='H:/cloud/cloud_data/Projects/DISCHARGEDB/src/scripts/set_primary_key.sql', replace=('TABLE_VAR','v_a06_docu_hosp'))
    
    
    result = db.executeSQL('SELECT * FROM dischargedb3.site;')
    db.sas7bdatTosql()
    db.closeSQL()
    
    filename = 'v_a01_fu_staff'
    
    
    db = DISCHARGEDB(database=settings['database'])
    db.connectSQL()
    db.executeSQL('ALTER TABLE ' + filename + ' ADD PRIMARY KEY index')
    
    command = "ALTER TABLE `dischargedb`.`v_a03_ses_staff` CHANGE COLUMN `index` `index` BIGINT NOT NULL ,"
    
    cursor = db.db.cursor()
    cursor.execute(command)
    result = cursor.fetchall()
    
    db = DISCHARGEDB(database=settings['database'])
    db.connectSQL()
    #command = "ALTER TABLE `dischargedb`.`v_a02_fu_questf_sub01` CHANGE COLUMN `index` `index` BIGINT NULL ,ADD PRIMARY KEY (`index`);;"
    command = "ALTER TABLE dischargedb.v_a03_ses_staff CHANGE COLUMN index index BIGINT NOT NULL"
    db.executeSQL(command)
    
                
    ##############################
    
    reader = SAS7BDAT('H:/cloud/cloud_data/Projects/DISCHARGEDB/data/tmp/ecrf/v_g02_ct_reading_a.sas7bdat', skip_header=False)
    df1 = reader.to_data_frame()
    for i in  range(len(reader.columns)):
        f = reader.columns[i].format
        print('format:', f)
    
    c = reader.columns[10]
    
    
    fip = 'H:/cloud/cloud_data/Projects/DISCHARGEDB/data/tmp/ecrf/v_a01_fu_staff.sas7bdat'
    df = pd.read_sas(fip, format = 'sas7bdat', encoding='iso-8859-1')
    df.to_sql(con=con, name='table_name_for_df', if_exists='replace', flavor='mysql')
    
    
    mysql_path = 'mysql://root:123@localhost/?charset=utf8'
    engine = create_engine(mysql_path, encoding="utf-8", echo=False)
    # with engine.connect() as con:
    # con.execute("use dischargedb3; drop table if exists " + name + ";")
    # df = pd.read_excel(path)
    # df.to_sql(name, engine, index=False)
    
    fip = 'H:/cloud/cloud_data/Projects/DISCHARGEDB/data/tables/sas/v_a02_fu_questf_sub01.sas7bdat'
    df = pd.read_sas(fip, format = 'sas7bdat', encoding='iso-8859-1')
    
    with engine.connect() as con:
        #con = engine.connect()
        con.execute("use dischargedb3;")
    df.to_sql('table6', engine, index=False)
    
    
    df = pd.read_excel('H:/cloud/cloud_data/Projects/DISCHARGEDB/data/tables/xlsx/discharge_ecrf_01092020.xlsx', sheet_name='Sheet1', index_col=0)




