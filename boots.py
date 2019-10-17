# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 16:24:12 2019

@author: Ativeer Patni
"""
#Boots
import numpy as np
import pandas as pd
import re
import glob, os
import pymsgbox
import sys
import datetime
print('**********Boots Begins**********')
pathh='\\\\SWUKGF46.ea.win.colpal.com\\CP\\Groups\\DATA\\Boots\\EPOS\\'
#sharing_path='C:\\Users\\ativeer patni\\Desktop\\UK\\'
today = datetime.date.today()
mon=today - datetime.timedelta(days=today.weekday())
tue=today - datetime.timedelta(days=today.weekday()-1)
mon1=mon.strftime('%Y%m%d')
tue1=tue.strftime('%Y%m%d')
mon2=pathh+mon.strftime('%Y\\')+mon1
tue2=pathh+tue.strftime('%Y\\')+tue1
chk=0
exists = os.path.exists(mon2)

if exists is True:
    print('Monday folder ready')
    day=mon1
    folder=mon2
    
elif os.path.exists(tue2) is True:
    print('Tuesday folder ready')
    folder=tue2
    day=tue1
    
else:
    print('Folder not ready')
    sys.exit('Check after sometime!')

#ValueError: max() arg is an empty sequence


sample_path='\\\\hydep0.esc.win.colpal.com\\posfiles\\POS\\Raw Files\\UK\\Archive\\Boots\\sample files\\boots_gb'
#sample_path='C:\\Users\\ativeer patni\\Desktop\\UK\\Boots historical extracts\\Sample\\boots_gb'
extracts_archive='\\\\hydep0.esc.win.colpal.com\\posfiles\\POS\\Raw Files\\UK\\Archive\\Boots\\extracts archive\\'+day+'boots_gb'

#extracts_archive='C:\\Users\\ativeer patni\\Desktop\\UK\\Boots historical extracts\\extracts archive\\'+day+'boots_gb'
extracts_path='\\\\hydep0.esc.win.colpal.com\\posfiles\\POS\\UK\\BOOTS\\Inbound\\boots_gb'#extracts file location

path1=folder+'\\*.xlsx'#raw file location
archive="\\\\hydep0.esc.win.colpal.com\\posfiles\\POS\\Raw Files\\UK\\Archive\\Boots\\raw file archive\\"
list_of_files = glob.glob(path1)
path = max(list_of_files, key=os.path.getctime)#file path along with file name
fname0=path.split('\\')[-1]#only file name
exists = os.path.exists(archive+fname0)
if exists:
    print('Boots Shared')
    pymsgbox.alert('Boots Shared',timeout=5000)
else:
    print('New File Found')


    rd=pd.ExcelFile(path)
    print('Reading Raw File')
    sheetnames=rd.sheet_names
    #volume sheet
    df=pd.read_excel(path,sheetnames[0])
    #value (sales) sheet
    df1=pd.read_excel(path,sheetnames[1])
    #finding ad date in cell
    row=df[df.apply(lambda r: r.str.contains('Ad Date').any(), axis=1)]
    row1=df1[df1.apply(lambda r: r.str.contains('Ad Date').any(), axis=1)]
    
    for k,i in enumerate(row.columns):
        if 'Ad Date' in str(row.loc[:,i]):
            #print('ok')
            break
    for k1,i1 in enumerate(row1.columns):
        if 'Ad Date' in str(row1.loc[:,i1]):
            #print('ok')
            break
    
    dates=row.shape[1]-k-2#removing grand total and Ad date column
    #for total number of dates
    date_arr=[]
    for i in range(dates):
        date_arr.append(row.iloc[:,k+i+1][row.index[0]])
        
    print('Found all dates')
    s_time=pd.read_csv(sample_path+'_time.csv')
    s_time['DATE_FROM']=s_time['DATE_TO']=date_arr
    s_time['DDAGR']='BOOTS'
    #********************************time extracts********************************************************
    
    print('Time Extracts Ready')
    #******************************MAIN EXTRACTS********************************************************
    df.columns=df.iloc[row.index[0]+1]
    df=df.drop(df.index[0:(row.index[0]+3)])
    df.reset_index(drop=True, inplace=True)
    #removed all rows above ad date and made a row below that as column header
    
    df1.columns=df1.iloc[row1.index[0]+1]
    df1=df1.drop(df1.index[0:(row1.index[0]+3)])
    df1.reset_index(drop=True, inplace=True)
    #same for value sheet
    
    #selecting 3 column names with decription, item number and barcode
    df0=pd.DataFrame({'Description':df['BOOTS ITEM DESC'],'Item':df['BOOTS ITEM NUM'],'Barcode':df['COLG UNIT BARCODE']})
    df0['Barcode']=[re.sub('^-$','',m1) for m1 in df0['Barcode'].astype(str)]
    #drop columns with all nan values
    df=df.dropna(how='all')
    df1=df1.dropna(how='all')
    #df.iloc[:,0]
    #removing extra columns
    df.drop(['BOOTS ITEM DESC','BOOTS ITEM NUM','COLG UNIT BARCODE'],axis=1,inplace=True)
    df1.drop(['BOOTS ITEM DESC','BOOTS ITEM NUM','COLG UNIT BARCODE'],axis=1,inplace=True)
    #df.shape[1]
    #len(date_arr)
    
    df.columns=np.arange(df.shape[1])
    if df.shape[1]-len(date_arr)==2:
        print('Extra column in front')
        df.drop([0],inplace=True,axis=1)
        df.drop([df.shape[1]],axis=1,inplace=True)
        print('Removing nan and Grand total column')
    elif df.shape[1]-len(date_arr)==1:
        print('Removing Grand Total Column')
        df.drop([df.shape[1]],axis=1,inplace=True)
    else:
        sys.exit('Column mismatch in Volume Sheet: Please Raise a CR')
    
    #****************exception considered only if a column is present in front
    
    df1.columns=np.arange(df1.shape[1])
    if df1.shape[1]-len(date_arr)==2:
        print('Extra column in front')
        df1.drop([0],inplace=True,axis=1)
        df1.drop([df1.shape[1]],axis=1,inplace=True)
        print('Removing nan and Grand total column')
    elif df1.shape[1]-len(date_arr)==1:
        print('Removing Grand Total Column')
        df1.drop([df1.shape[1]],axis=1,inplace=True)
    else:
        sys.exit('Column mismatch in Sales Sheet: Please Raise a CR')
    
    
    df.columns=date_arr
    df1.columns=date_arr
    #header is all dates in raw file
    s_main=pd.read_csv(sample_path+'.csv')
    #volume
    col_name='CPQUASU'
    #sales
    col_name1='CPSVLC'
    ln=len(df[date_arr])
    s0=pd.Series(None)
    s1=pd.Series(None)
    s2=pd.Series(None)
    s3=pd.Series(None)
    for ind,i in enumerate(df.columns):
        s3=s3.append(df0['Item'],ignore_index=True)
        s2=s2.append(df1[date_arr[ind]],ignore_index=True)
        s0=s0.append(df[date_arr[ind]],ignore_index=True)
        dts=pd.Series([i]*ln)
        s1=s1.append(dts,ignore_index=True)
        
    
    s_main[col_name]=pd.Series(s0)
    s_main[col_name]=[re.sub('-$','0',m1) for m1 in s_main[col_name].astype(str)]
    s_main['DATEFROM']=s1
    s_main['DATETO']=s1
    s4='GB_'+s3
    s_main['/1DD/S_PRODREF']=s4
    s_main['/1DD/S_LOCREF']='BOOTS_GB'
    s_main['/1DD/S_LOCTYPE']='1040'
    s_main['/1DD/S_SALGRP']='1000'
    s_main['/1DD/S_TIMAGGRLV']='W'
    s_main[col_name1]=s2
    s_main[col_name1]=[re.sub('-$','0',m2) for m2 in s_main[col_name1].astype(str)]
    s_main['SALES_UNIT']='CU'
    
    
    print('boots_gb Extract Ready')
    #******************************prod file******************************************
    #Already maintained data
    df2=pd.read_excel(sample_path+'_mapped_data.xlsx')
    
    df2['Description']=df2['Description'].str.lower()
    df0['Description']=df0['Description'].str.lower()
    df3=pd.merge(df0,df2,on=['Description'],how='left')
    
    print('New records:',df3['Variant'].isnull().sum())
    nr=df3['Variant'].isnull().sum()
    if nr!=0:
          extracts_path='\\\\hydep0.esc.win.colpal.com\\posfiles\\POS\\Raw Files\\UK\\Archive\\Boots\\Validation\\boots_gb'
          #extracts_path='C:\\Users\\ativeer patni\\Desktop\\UK\\Boots historical extracts\\Validation\\boots_gb'
          msg1='New records: ',nr,' Press Ok to proceed folder path: \\\\hydep0.esc.win.colpal.com\\posfiles\\POS\\Raw Files\\UK\\Archive\\Boots\\Validation\\boots_gb'
          #msg1='New records: ',nr,' Press Ok to proceed; folder path for extracts: C:\\Users\\ativeer patni\\Desktop\\UK\\Boots historical extracts\\Validation\\'
          #ans=pymsgbox.confirm(msg1,timeout=10000)
          ans=pymsgbox.confirm(msg1,'Confrim',['Ok','Cancel'],timeout=10000)
          if ans=='Cancel':
              sys.exit('Validation Cancelled')
    
    s_prod=pd.read_csv(sample_path+'_prod.csv')
    
    s_prod['DESCRIPTION']=df0['Description'].str.capitalize()
    s_prod['EXTERNAL_KEY']='GB_'+df0['Item']
    s_prod['EANUPC']=df0['Barcode']
    s_prod['BASE_UOM']='CU'
    s_prod['/BIC/ZMAPPING']='GB_'+df0['Barcode']
    s_prod['ZRETSKU']=df0['Item']
    s_prod['ZCATEGRY']=df3['ZCATEGRY']
    s_prod['ZSUBCATS']=df3['ZSUBCATS']
    s_prod['ZSUBBR']=df3['ZSUBBR']
    s_prod['ZGPVAR']=df3['ZGPVAR']
    s_prod['ZPRCATEG']=df3['ZPRCATEG']
    s_prod['ZBREQTY']=df3['ZBREQTY']
    s_prod['COUNTRY']='GB'
    
    print('Prod extract ready')
    
    #*****************GEOGRAPHY***************************
    s_geo=pd.read_csv(sample_path+'_geog.csv')
    s_geo.to_csv(extracts_path+'_geog.csv',index=False)
    s_time.to_csv(extracts_path+'_time.csv',index=False)
    s_main.to_csv(extracts_path+'.csv',index=False)
    s_prod.to_csv(extracts_path+'_prod.csv',index=False)
    
    if nr==0:
        print('Archiving Extracts')
        s_time.to_csv(extracts_archive+'_time.csv',index=False)
        s_main.to_csv(extracts_archive+'.csv',index=False)
        s_prod.to_csv(extracts_archive+'_prod.csv',index=False)
        s_geo.to_csv(extracts_archive+'_geog.csv',index=False)
    
    
    
    
    import shutil
    
    shutil.copy(path,archive+fname0)
    print('Raw file Archived')

print('**************Boots Ends****************')