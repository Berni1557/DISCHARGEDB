# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 18:47:52 2021

@author: bernifoellmer
"""
import os, time, sys
import distutils.log, distutils.dir_util
import pandas as pd
from glob import glob
from helper import splitFolderPath
distutils.log.set_verbosity(distutils.log.DEBUG)

def uid_down(uids, fp_download):   
    #cmd1 = 'google-chrome "https://judi.agmednet.net/storag/discharge/wado?requestType=WADO&studyUID='
    cmd1 = 'chrome "https://judi.agmednet.net/storag/discharge/wado?requestType=WADO&studyUID='
    cmd2 = '&seriesUID=&objectUID=&contentType=application/x-zip-compressed"'  
    options = ''   
    sleep_seconds = 10
    
    if not os.path.isdir(fp_download):
        raise ValueError("Download directory " + fp_download + ' does not exist!')
        
    for uid in uids: 
        print('uid', uid)                      
        try:
            down_cmd = cmd1 + uid + cmd2 + options
            print(uid)
            os.system(down_cmd)           
            time.sleep(sleep_seconds)       
        except:
            print("Unexpected error:", sys.exc_info()[0])
           
        while any("crdownload" in s for s in glob(fp_download + '/*')):                               
            try:
                print ('#',end='')
                time.sleep(sleep_seconds)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                print("Unexpected error:", sys.exc_info()[0])       
        time.sleep(sleep_seconds)
        target_file = os.path.join(fp_download,uid+'.zip')
        if os.path.isfile(target_file):         
            print ('++')
            time.sleep(sleep_seconds)
            continue
        print('FAILED')
        
def mednet_down(fp_download='H:/cloud/cloud_data/Projects/DISCHARGEDB/agmednet/images', fp_images='G:/discharge', fip_report='H:/cloud/cloud_data/Projects/DISCHARGEDB/agmednet/report/AG_Mednet_Report_20210420_0805.xlsx'):
    df = pd.read_excel(fip_report, sheet_name = 'Sheet 1') 
    df = df[df['Transmission Status']=='SUCCESS']
    df.drop_duplicates('Study Instance UID', inplace=True, keep='first')
    uids_exist =  [splitFolderPath(f)[1] for f in glob(fp_images + '/*')]
    uids_exist_idx = df['Study Instance UID'].isin(uids_exist)
    df_down = df[~uids_exist_idx]
    uids = df_down['Study Instance UID'].tolist()  
    print('Downloading ' + str(len(uids)) + ' images.')
    uid_down(uids, fp_download)
    
#mednet_down(fp_download='G:/discharge',fip_report='H:/cloud/cloud_data/Projects/DISCHARGEDB/agmednet/report/AG_Mednet_Report_20210420_0805.xlsx')
   
