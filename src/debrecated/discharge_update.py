#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 27 12:11:03 2020

@author: lukass
"""

import os, time, sys, shutil, json
import pandas as pd
from discharge_extract import extract_specific_tags_df, df_to_excel

def recursive_overwrite(src, dest, ignore = None):
	if os.path.isdir(src):
		if not os.path.isdir(dest):
			os.makedirs(dest)
		files = os.listdir(src)
		if ignore is not None:
			ignored = ignore(src, files)
		else:
			ignored = set()
		for f in files:
			if f not in ignored:
				recursive_overwrite(os.path.join(src, f), os.path.join(dest, f), ignore)
	else:
		if os.path.exists(dest):
			if os.path.samefile(src, dest):
				return			
			os.remove(dest)
		shutil.copy2(src, dest)

def mednet_down():	
	f = '/home/lukass/Downloads/AG_Mednet_Report_20201030_1706.xlsx'
	df = pd.read_excel(f, sheet_name = 'Sheet 1')	 
	uids = df['Study Instance UID'].tolist()	
	uid_down(uids)
	
def uid_down(uids):	
	cmd1 = 'google-chrome "https://judi.agmednet.net/storag/discharge/wado?requestType=WADO&studyUID='
	cmd2 = '&seriesUID=&objectUID=&contentType=application/x-zip-compressed"'       	
	#options = ' --disable-setuid-sandbox --no-sandbox  --disable-logging '	
	options = ''	
	sleep_seconds = 5
	target_dir = '/home/lukass/Downloads/'
		
	for i, uid in enumerate(uids):            
		print(i,'/',len(uids),':',uid,end=',')		
		target_file = os.path.join(target_dir,uid+'.zip')
		if os.path.isfile(target_file):          
			print ('++')			
			continue
		try:			
			down_cmd = cmd1 + uid + cmd2 + options			
			os.system(down_cmd)            
			time.sleep(sleep_seconds)        
		except:
			print("Unexpected error:", sys.exc_info()[0])
			
		while any("crdownload" in s for s in os.listdir(target_dir)):                        		
			try:
				print ('#',end='')
				time.sleep(sleep_seconds)
			except (KeyboardInterrupt, SystemExit):
				raise
			except:
				print("Unexpected error:", sys.exc_info()[0])		
		
		if os.path.isfile(target_file):          
			print ('++')
			time.sleep(sleep_seconds)
			continue
		print('--')
		 
def update(src, dst, dry = 0):    	
	uids = os.listdir(src)      
	for uid in uids:       
		f0,f1 = os.path.join(src, uid), os.path.join(dst, uid)        		
		n0 = len(os.listdir(f0))
		n1 = len(os.listdir(f1)) if os.path.exists(f1) else 0
		print(uid, n0, n1)    
        
		if dry: continue
		recursive_overwrite(f0, f1)

def update2(root_src_dir, root_dst_dir, dry = 0):
	for src_dir, dirs, files in os.walk(root_src_dir):
		dst_dir = src_dir.replace(root_src_dir, root_dst_dir, 1)
		print(src_dir, ' > ', dst_dir)
		
		n0 = len(os.listdir(src_dir))
		n1 = len(os.listdir(dst_dir)) if os.path.exists(dst_dir) else 0
		print(n0, n1)   
		#print(uid, n0, n1)   
		
		s = os.path.normpath(src_dir).split(os.sep)
		#path.split(os.sep)
		print(s)
		#uid = src_dir.split('/')
		return    
		if dry: continue
	
		if not os.path.exists(dst_dir):
			#print('makedir')
			os.makedirs(dst_dir)
		for file_ in files:
			src_file = os.path.join(src_dir, file_)
			dst_file = os.path.join(dst_dir, file_)
			if os.path.exists(dst_file):
	            # in case of the src and dst are the same file
				if os.path.samefile(src_file, dst_file):
					#print('identical')
					continue
				#print('remove')
				os.remove(dst_file)
			shutil.copy2(src_file, dst_dir)  #also meatadata
			#print('copy')


def new_tags(root, fout, uids=None ,ftags=None):		
    if not uids:
        uids = os.listdir(root)	
    if not ftags:
        ftags = 'dischargetagsall.txt'		                
    with open(ftags , "r") as fp: 
        selected_tags = json.load(fp)			
    df =  extract_specific_tags_df(selected_tags, root, uids)
    df.sort_values('PatientID', inplace=True)
    df.reset_index(drop=True, inplace=True)        	
    df2 = df.set_index(['PatientID','StudyInstanceUID','SeriesInstanceUID'])    
	#df2 = df.set_index(['PatientID','StudyInstanceUID','SeriesInstanceUID','SOPInstanceUID'])
    df2.sort_index(inplace = True)  
    writer = pd.ExcelWriter(fout, engine = 'xlsxwriter')     
    df_to_excel(writer, "ordered", df2)    
    df_to_excel(writer, "linear", df)    
    writer.save()

def save_tags():
	f = '/home/lukass/Downloads/discharge_tags_15092020_all.xlsx'
	df = pd.read_excel(f, 'linear')    
	del df['Unnamed: 0']	
	l = df.columns.tolist()	
	ft = "test.txt"
	with open(ft, "w") as fp:
		json.dump(l, fp)	
	with open(ft, "r") as fp:
		l2 = json.load(fp)
	print(l==l2)

def merge_tags(fold,fnew,fout):
	print('read')	
	df1 = pd.read_excel(fold, 'linear')    	
	df2 = pd.read_excel(fnew, 'linear')    
	uids = df2['StudyInstanceUID'].unique().tolist()	
	print('drop')
	for uid in uids:		
		print(uid)
		df1.drop(df1.index[df1['StudyInstanceUID'] == uid], inplace = True)	
	print('append')	
	df = df1.append(df2, verify_integrity = True, ignore_index = True)		
	df.sort_values('PatientID', inplace = True)
	df.reset_index(drop = True, inplace = True)  		
	del df['Unnamed: 0']	
	dff = df.set_index(['PatientID','StudyInstanceUID','SeriesInstanceUID'])    	
	dff.sort_index(inplace = True)    	    	
	print('write')		
	writer = pd.ExcelWriter(fout, engine = 'xlsxwriter')     	
	df_to_excel(writer, "ordered", dff)    
	df_to_excel(writer, "linear", df)    	
	writer.save()	

def compare_df():
	f1 = 'tags1.xlsx'
	f2 = 'tags3.xlsx'	
	df1 = pd.read_excel(f1, 'linear')    
	df2 = pd.read_excel(f2, 'linear')    
	d = df1.compare(df2)
	print(d)
	
def compare_df_():
	f1 = 'tags_new1.xlsx'	
	f2 = 'tags_new2.xlsx'		
	df1 = pd.read_excel(f1, 'linear')    	
	df2 = pd.read_excel(f2, 'linear')    	
	l1 = df1.columns.tolist()
	l2 = df2.columns.tolist()
	#print(l1==l2)
	#set_difference = set(l1) - set(l2)
	set_difference = set(l1) - set(l2)
	print(list(set_difference))

def compare_dir():
	import filecmp
	d1 = '/home/lukass/Downloads/temp/1.2.124.113532.80.22205.16961.20161110.103501.3234415992'
	d1 = '/home/lukass/Downloads/temp/1.2.124.113532.80.22205.16961.20161110.103501.3234415992/1.3.46.670589.33.1.63614379889856888700001.5678457401671765526'	
	d2 = '/media/lukass/discharge1/discharge_dcm/1.2.124.113532.80.22205.16961.20161110.103501.3234415992'
	d2 = '/media/lukass/discharge1/discharge_dcm/1.2.124.113532.80.22205.16961.20161110.103501.3234415992/1.3.46.670589.33.1.63614379889856888700001.5678457401671765526'
	result = filecmp.dircmp(d1,d2)
	#print(result.report())

	ftags = 'dischargetagsall.txt'		
	with open(ftags , "r") as fp:
		selected_tags = json.load(fp)	

	#extract_specific_tags_df(specific_tags, root, suids=[]):	
	root1 = '/home/lukass/Downloads/temp'	
	root2 = '/media/lukass/discharge1/discharge_dcm'
	
	#d1 = '/home/lukass/Downloads/temp/1.2.124.113532.80.22205.16961.20161110.103501.3234415992'
	#d2 = '/media/lukass/discharge1/discharge_dcm/1.2.124.113532.80.22205.16961.20161110.103501.3234415992'
	
	#uids = ['1.2.124.113532.80.22205.16961.20161110.103501.3234415992']
	uids = os.listdir(root1)

	df =  extract_specific_tags_df(selected_tags, root2, uids)    	
	
	df.sort_index(inplace = True)          
	df2 = df.set_index(['PatientID','StudyInstanceUID','SeriesInstanceUID'])    	
	df2.sort_index(inplace = True)    	    

	fout = 'tags2.xlsx'
	writer = pd.ExcelWriter(fout, engine = 'xlsxwriter')     
	df_to_excel(writer, "ordered", df2)    
	df_to_excel(writer, "linear", df)    
	writer.save()
	
def count_studies():	
    df = pd.read_excel('/home/lukass/Downloads/discharge_27102020.xlsx', 'linear')    		
    uids = df['StudyInstanceUID'].unique().tolist()
    print(len(uids))
	
if __name__=='__main__':      
    start_time = time.time()  

    #1 newtags 2 update 3 merge

    if 0:
        pass
    	#mednet_down()
    
   

    if 0:
	    root = '/home/lukass/Downloads/temp'	
	    fout = '/home/lukass/Downloads/temp.xlsx'		    
	    new_tags(root,fout)

    if 0:	
        pass
        #update('D:\\temp','D:\\discharge_dcm', 0)    	
        #update('/home/lukass/Downloads/temp','/media/lukass/discharge1/discharge_dcm', 0)
	

    if 1:
        fold = '/home/lukass/Downloads/discharge_14012021.xlsx'
        fnew = '/home/lukass/Downloads/temp.xlsx'	
        fout = '/home/lukass/Downloads/discharge_19022021.xlsx'
        merge_tags(fold,fnew,fout)
	
	#compare_dir()	
	#check_diffs()
	#compare_df()
	#count_studies()

	
    print("--- %s seconds ---" % (time.time() - start_time))

