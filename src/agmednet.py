# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 18:47:52 2021

@author: bernifoellmer
"""
import os, time, sys
import distutils.log, distutils.dir_util
import pandas as pd
from glob import glob
distutils.log.set_verbosity(distutils.log.DEBUG)

def uid_down(uids):   
    #cmd1 = 'google-chrome "https://judi.agmednet.net/storag/discharge/wado?requestType=WADO&studyUID='
    cmd1 = 'chrome "https://judi.agmednet.net/storag/discharge/wado?requestType=WADO&studyUID='
    cmd2 = '&seriesUID=&objectUID=&contentType=application/x-zip-compressed"'          
    #options = ' --disable-setuid-sandbox --no-sandbox  --disable-logging '   
    options = ''   
    sleep_seconds = 5
    #target_dir = '/home/lukass/Downloads/'
    download_dir = 'H:/cloud/cloud_data/Projects/DISCHARGEDB/data/images'
    #download_dir = 'C:/Users/bernifoellmer/Download'
   
    for uid in uids:                       
        try:
            down_cmd = cmd1 + uid + cmd2 + options
            print(uid)
            os.system(down_cmd)           
            time.sleep(sleep_seconds)       
        except:
            print("Unexpected error:", sys.exc_info()[0])
           
        while any("crdownload" in s for s in glob(download_dir + '/*')):                               
            try:
                print ('#',end='')
                time.sleep(sleep_seconds)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                print("Unexpected error:", sys.exc_info()[0])       
        target_file = os.path.join(download_dir,uid+'.zip')
        if os.path.isfile(target_file):         
            print ('++')
            time.sleep(sleep_seconds)
            continue
        print('FAILED')
        
def mednet_down():
    #f ='/home/lukass/Downloads/AG_Mednet_Report_20201019_1058.xlsx'   
    f = 'H:/cloud/cloud_data/Projects/DISCHARGEDB/data/tables/xlsx/discharge_master_01092020.xlsx'
    df = pd.read_excel(f, sheet_name = 'MASTER_01092020')    
    uids = df['StudyInstanceUID'].tolist()   
    uid_down(uids)
   
