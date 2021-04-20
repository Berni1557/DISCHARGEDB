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
import mysql.connector
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from settings import initSettings, saveSettings, loadSettings, fillSettingsTags
import xlrd
from helper import splitFilePath
from agmednet import mednet_down

class DISCHARGEDB:
    host = ''
    port = ''
    user = ''
    password = ''
    database = ''
    db = None
    
    def __init__(self, host="127.0.0.1", port='3306', user="root", password="123", database='dischargedb3'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.db = None
        
    def download_images(self, settings):
        mednet_down(settings['fp_download'], settings['fp_images'], settings['fip_report'])
    
    def createDB(self):
        command_create = "CREATE DATABASE if not exists " + self.database
        self.db.cursor().execute(command_create)
        # Init database
        command = "SET @@global.sql_mode= '';"
        self.db.cursor().execute(command)
        
    def initeDB(self, settings):
        # Upload xlsx files
        files = glob(settings['folderpath_xlsx'] + '/*.xlsx')
        for file in files:
            print('Upload: ' + file)
            self.xlxsTosql(database=self.database, 
                            ip=self.host, 
                            port=self.port, 
                            un=self.user, 
                            pw=self.password, 
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
        self.db = mysql.connector.connect(
          host=self.host,
          user=self.user,
          password=self.password,
          database=self.database
        )
        
    def executeSQL(self, command='SELECT * FROM dischargedb3.site;'):
        try:
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
            
    def xlxsTosql(self, database='dischargedb3', ip='localhost', port='3306', un='root', pw='123', fip_xlsx='H:/cloud/cloud_data/Projects/DISCHARGEDB/data/tables/xlsx/discharge_master_01092020.xlsx'):
        try:
            xls = xlrd.open_workbook(fip_xlsx)
            sheet_names = xls.sheet_names()
            for sheet_name in sheet_names:
                df = pd.read_excel(fip_xlsx, sheet_name=sheet_name)
                df.columns = df.columns.astype(str)
                mysql_path = 'mysql://' + un + ':' + pw + '@' + ip + '/'+ database + '?charset=utf8'
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

        
#if __name__=='__main__':   
def main():
    # Load settings
    filepath_settings = 'H:/cloud/cloud_data/Projects/DISCHARGEDB/code/data/settings.json'
    settings=initSettings()
    saveSettings(settings, filepath_settings)
    settings = fillSettingsTags(loadSettings(filepath_settings))
        
    #### Downlaod new image #####
    db = DISCHARGEDB(database=settings['database'])
    db.download_images(settings)
    
    #### Execute sript #####
    db = DISCHARGEDB(database=settings['database'])
    db.connectSQL()
    db.createDB()
    db.initeDB(settings)
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




