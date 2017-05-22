# -*- coding: utf-8 -*-
# *** Spyder Python Console History Log ***

##---(Tue May 09 14:07:00 2017)---
import numpy as np
import pandas as pd
list(open('jh_result.txt'))
data = pd.read_table('jh_result.txt',sp='|'))
data = pd.read_table('jh_result.txt',sp='|')
data = pd.read_table('jh_result.txt',sep='|')
data
type(data)
pd.isnull(data)
data
data = pd.read_table('jh_result.txt',sep='|',names=['PRODUCT_NO'
,'CUST_NAME'
,'USER_ID'
,'SEX'
,'AGE'
,'STAR_LVL'
,'ARPU'
,'CITY_ID'
,'COUNTY_ID'
,'INNET_DUR'
,'NEW_REST_LAC_CI'
,'STAY_LAC_CI'
,'STAY_DAYS'
,'ZJUSER_MARK'
,'NEW_REST_FLAG'
,'NIGHT_WIFI_USE_FLAG'
,'WORK_LAC_CI'
,'WORK_DAYS'
,'YD_KD_FLAG'
,'KD_RATE'
,'CMCC_KD_CNT'
,'CMCC_KD_RATE'
,'HIGH_ARPU_FLAG'
,'MAIN_QQW_FLAG'
,'WEEK_NEW_REST_FLAG'
,'WEEK_NEW_REST_LAC_CI'
,'WIFI_USE_FLAG'
,'AGE_RATE'
,'EXPR_FLAG'
,'RATE_UP'
,'DS_FLAG'
,'DSBL_FLAG'
,'GROUP_PLAN_FLAG'
,'STAY_CITY_ID'
,'STAY_CITY_NAME'
,'STAY_COUNTY_ID'
,'STAY_COUNTY_NAME'
,'STAY_LACCI_NAME'
,'BEF_LAC_CI'
,'BEF_CITY_ID'
,'BEF_CITY_NAME'
,'BEF_STAY_DAYS'
,'MID_LAC_CI'
,'MID_CITY_ID'
,'MID_CITY_NAME'
,'MID_STAY_DAYS'
,'AFT_LAC_CI'
,'AFT_CITY_ID'
,'AFT_CITY_NAME'
,'AFT_STAY_DAYS'
,'CLUSTER_ID'
,'AFT_BEF_DIST'
,'SEARCH_FLAG'
,'SEARCH_KW_1'
,'SEARCH_KW_2'
,'SEARCH_KW_3'
,'SEARCH_KW_4'
,'SEARCH_KW_5'
,'SEARCH_ZF_FLAG'
,'SEARCH_BJ_FLAG'
,'SEARCH_ZJ_FLAG'
,'TOTAL_FLUX'
,'NIGHT_GPRS_RATE'
,'NOWIFI_DAY_CNT'
,'HOME_GPRS'
,'WORK_GPRS'
,'HIGH_HOME_FLAG'
,'HOME_GPRS_RATE'
,'HOME_DEPEND'
,'ALL_ALL_GPRS'
,'SEARCH_DATE_ID'
,'AGE_FLAG1'
,'AGE_FLAG2'
,'STAR_LVL1'
,'STAR_LVL2'
,'STAR_LVL3'
,'P'
,'LABEL'])
data
data = pd.read_table('jh_result.txt',sep='|',index_col=['PRODUCT_NO'],names=[
'CUST_NAME'
,'USER_ID'
,'SEX'
,'AGE'
,'STAR_LVL'
,'ARPU'
,'CITY_ID'
,'COUNTY_ID'
,'INNET_DUR'
,'NEW_REST_LAC_CI'
,'STAY_LAC_CI'
,'STAY_DAYS'
,'ZJUSER_MARK'
,'NEW_REST_FLAG'
,'NIGHT_WIFI_USE_FLAG'
,'WORK_LAC_CI'
,'WORK_DAYS'
,'YD_KD_FLAG'
,'KD_RATE'
,'CMCC_KD_CNT'
,'CMCC_KD_RATE'
,'HIGH_ARPU_FLAG'
,'MAIN_QQW_FLAG'
,'WEEK_NEW_REST_FLAG'
,'WEEK_NEW_REST_LAC_CI'
,'WIFI_USE_FLAG'
,'AGE_RATE'
,'EXPR_FLAG'
,'RATE_UP'
,'DS_FLAG'
,'DSBL_FLAG'
,'GROUP_PLAN_FLAG'
,'STAY_CITY_ID'
,'STAY_CITY_NAME'
,'STAY_COUNTY_ID'
,'STAY_COUNTY_NAME'
,'STAY_LACCI_NAME'
,'BEF_LAC_CI'
,'BEF_CITY_ID'
,'BEF_CITY_NAME'
,'BEF_STAY_DAYS'
,'MID_LAC_CI'
,'MID_CITY_ID'
,'MID_CITY_NAME'
,'MID_STAY_DAYS'
,'AFT_LAC_CI'
,'AFT_CITY_ID'
,'AFT_CITY_NAME'
,'AFT_STAY_DAYS'
,'CLUSTER_ID'
,'AFT_BEF_DIST'
,'SEARCH_FLAG'
,'SEARCH_KW_1'
,'SEARCH_KW_2'
,'SEARCH_KW_3'
,'SEARCH_KW_4'
,'SEARCH_KW_5'
,'SEARCH_ZF_FLAG'
,'SEARCH_BJ_FLAG'
,'SEARCH_ZJ_FLAG'
,'TOTAL_FLUX'
,'NIGHT_GPRS_RATE'
,'NOWIFI_DAY_CNT'
,'HOME_GPRS'
,'WORK_GPRS'
,'HIGH_HOME_FLAG'
,'HOME_GPRS_RATE'
,'HOME_DEPEND'
,'ALL_ALL_GPRS'
,'SEARCH_DATE_ID'
,'AGE_FLAG1'
,'AGE_FLAG2'
,'STAR_LVL1'
,'STAR_LVL2'
,'STAR_LVL3'
,'P'
,'P_DAY'
,'LABEL'])
data = pd.read_table('jh_result.txt',sep='|',names=[
'CUST_NAME'
,'USER_ID'
,'SEX'
,'AGE'
,'STAR_LVL'
,'ARPU'
,'CITY_ID'
,'COUNTY_ID'
,'INNET_DUR'
,'NEW_REST_LAC_CI'
,'STAY_LAC_CI'
,'STAY_DAYS'
,'ZJUSER_MARK'
,'NEW_REST_FLAG'
,'NIGHT_WIFI_USE_FLAG'
,'WORK_LAC_CI'
,'WORK_DAYS'
,'YD_KD_FLAG'
,'KD_RATE'
,'CMCC_KD_CNT'
,'CMCC_KD_RATE'
,'HIGH_ARPU_FLAG'
,'MAIN_QQW_FLAG'
,'WEEK_NEW_REST_FLAG'
,'WEEK_NEW_REST_LAC_CI'
,'WIFI_USE_FLAG'
,'AGE_RATE'
,'EXPR_FLAG'
,'RATE_UP'
,'DS_FLAG'
,'DSBL_FLAG'
,'GROUP_PLAN_FLAG'
,'STAY_CITY_ID'
,'STAY_CITY_NAME'
,'STAY_COUNTY_ID'
,'STAY_COUNTY_NAME'
,'STAY_LACCI_NAME'
,'BEF_LAC_CI'
,'BEF_CITY_ID'
,'BEF_CITY_NAME'
,'BEF_STAY_DAYS'
,'MID_LAC_CI'
,'MID_CITY_ID'
,'MID_CITY_NAME'
,'MID_STAY_DAYS'
,'AFT_LAC_CI'
,'AFT_CITY_ID'
,'AFT_CITY_NAME'
,'AFT_STAY_DAYS'
,'CLUSTER_ID'
,'AFT_BEF_DIST'
,'SEARCH_FLAG'
,'SEARCH_KW_1'
,'SEARCH_KW_2'
,'SEARCH_KW_3'
,'SEARCH_KW_4'
,'SEARCH_KW_5'
,'SEARCH_ZF_FLAG'
,'SEARCH_BJ_FLAG'
,'SEARCH_ZJ_FLAG'
,'TOTAL_FLUX'
,'NIGHT_GPRS_RATE'
,'NOWIFI_DAY_CNT'
,'HOME_GPRS'
,'WORK_GPRS'
,'HIGH_HOME_FLAG'
,'HOME_GPRS_RATE'
,'HOME_DEPEND'
,'ALL_ALL_GPRS'
,'SEARCH_DATE_ID'
,'AGE_FLAG1'
,'AGE_FLAG2'
,'STAR_LVL1'
,'STAR_LVL2'
,'STAR_LVL3'
,'P'
,'P_DAY'
,'LABEL'])
data
data.describ()
data.describe()
import matplotlib as plt
data['COUNTY_ID'].hist(bins=10)
data['COUNTY_ID']
data['COUNTY_ID'].hist(bins=9)
data['LABEL'].hist(bins=2)
data['HIGH_ARPU_FLAG'].hist(bins=2)
data.boxplot(column='P')
data.index
type(data)
data.info()
range(79145)
num_index = range(79145)
data.index = num_index
data
data['ARPU'].hist[bins=5]
data['ARPU'].hist(bins=5)
data.dtype()
data.dtype
data.dtypes
data[data.LABEL=='1']
data_p = data[data.LABEL==1]
data_p
data.corr()
data[data.isnull()]
data[data.isnull()].index
data_clean = data.dropna()
data = data_clean
data
data.corr()
data_p['ARPU']
data_p['ARPU'].astype(int32)
data_p['ARPU'].astype(np.int32)
data_p[data.ARPU =='\N']
data_p[data.ARPU ='\N']
data_p[data.ARPU =='\\N']
data_p[data_p.ARPU =='\\N']
data_p[data_p.ARPU =='\\N'].replace('\\N','0')
data_p[data_p.ARPU =='\\N']
data_p[data_p.ARPU =='\\N'].replace('\N','0')
data_p[data_p.ARPU =='\\N']
data_p['50154'].ARPU
data_p[50154].ARPU
data_p[ix ='50154'].ARPU
data_p[ix =='50154'].ARPU
data_p[index =='50154'].ARPU
data_p[data_p.index =='50154'].ARPU
data_p[data_p.index ='50154'].ARPU
data_p[data_p.index =='50154'].ARPU
data_p[data_p.index ==50154].ARPU
data_p[data_p.index ==50154].ARPU='0'
data_p[data_p.index ==50154].ARPU
data_p.dropna()
data_p.replace('\N',0)
data_p
data_1 = data_p.replace('\N',0)
data_1.replace(NULL,0)
data_1.replace(None,0)
data_1.replace(None,'0')
data_1.replace(None,'\N')
data_1.replace('','\N')
data_2 = data_1.replace('','0')
data_2
data_2.corr()
data_2.info
data_2.info()
data_2.astype(np.int64)
data_1.corr()
data_p.corr()
data.corr()
data
data_p.AFT_LAC_CI
data[data_p.AFT_LAC_CI<>'\\N'].AFT_LAC_CI
data[data_p.AFT_LAC_CI!='\\N'].AFT_LAC_CI
data[data_p.AFT_LAC_CI!='\N'].AFT_LAC_CI
data_p[data_p.AFT_LAC_CI!='\\N'].AFT_LAC_CI
data_p[data_p.AFT_LAC_CI!='\\N'].AFT_LAC_CI.split('_')
list(data_p[data_p.AFT_LAC_CI!='\\N'].AFT_LAC_CI).split('_')
data_p[data_p.AFT_LAC_CI!='\\N'].AFT_LAC_CI
data_p[data_p.AFT_LAC_CI!='\\N'].AFT_LAC_CI.str
a = data_p[data_p.AFT_LAC_CI!='\\N'].AFT_LAC_CI.str.split('_')
a
a.info()
b = DataFrame(a)
a.index()
a
a[79143]
dataframe(a)
b = pd.DataFrame(a)
b
a
a.values()
b.values()
a.values
x = b.values
x
x = a.values
x
type(x)
x[0]
x[:][0]
x[1,2]
x[2]
x[2][1]
x[][1]
a.index
x[1:49192][1]
x[1:49192]
x[1:49192][0]
list(x)
c = list(x)
c[:][0]
c(1:49192)
c(:49192)
c[:49192][0]
y= list(b.dropna())
y
y= list(a.dropna())
y
c,d = y[i][0],y[i][1] for i in range(y.length)
def cf(x)
y.length
y.length()
len(y)
def cf(x):
    for i in range(len(x)):
        a[i] = x[i][0]
        b[i] = x[i][1]
    return a,b

cf(y)
def cf(x):
    for i in range(len(x)):
        if 
[x[i] = y[i][0] for i in range(len(y)]



f
f
fd:
fdf 
dfd 







]
[x[i] = y[i][0] for i in range(len(y))]
def cf(x):
    for i in range(len(x)):
        print i
        a[i] = x[i][0]
    return a

cf(y)
def cf(x):
    for i in range(len(x)):
        a[i] = x[i][0]
        b[i] = x[i][1]
    return a,b

cf(y)
data[data.LABEL == 0]
data[data.LABEL == 1]
data = pd.read_table('jh_result.txt',sep='|',names=[
'CUST_NAME'
,'USER_ID'
,'SEX'
,'AGE'
,'STAR_LVL'
,'ARPU'
,'CITY_ID'
,'COUNTY_ID'
,'INNET_DUR'
,'NEW_REST_LAC_CI'
,'STAY_LAC_CI'
,'STAY_DAYS'
,'ZJUSER_MARK'
,'NEW_REST_FLAG'
,'NIGHT_WIFI_USE_FLAG'
,'WORK_LAC_CI'
,'WORK_DAYS'
,'YD_KD_FLAG'
,'KD_RATE'
,'CMCC_KD_CNT'
,'CMCC_KD_RATE'
,'HIGH_ARPU_FLAG'
,'MAIN_QQW_FLAG'
,'WEEK_NEW_REST_FLAG'
,'WEEK_NEW_REST_LAC_CI'
,'WIFI_USE_FLAG'
,'AGE_RATE'
,'EXPR_FLAG'
,'RATE_UP'
,'DS_FLAG'
,'DSBL_FLAG'
,'GROUP_PLAN_FLAG'
,'STAY_CITY_ID'
,'STAY_CITY_NAME'
,'STAY_COUNTY_ID'
,'STAY_COUNTY_NAME'
,'STAY_LACCI_NAME'
,'BEF_LAC_CI'
,'BEF_CITY_ID'
,'BEF_CITY_NAME'
,'BEF_STAY_DAYS'
,'MID_LAC_CI'
,'MID_CITY_ID'
,'MID_CITY_NAME'
,'MID_STAY_DAYS'
,'AFT_LAC_CI'
,'AFT_CITY_ID'
,'AFT_CITY_NAME'
,'AFT_STAY_DAYS'
,'CLUSTER_ID'
,'AFT_BEF_DIST'
,'SEARCH_FLAG'
,'SEARCH_KW_1'
,'SEARCH_KW_2'
,'SEARCH_KW_3'
,'SEARCH_KW_4'
,'SEARCH_KW_5'
,'SEARCH_ZF_FLAG'
,'SEARCH_BJ_FLAG'
,'SEARCH_ZJ_FLAG'
,'TOTAL_FLUX'
,'NIGHT_GPRS_RATE'
,'NOWIFI_DAY_CNT'
,'HOME_GPRS'
,'WORK_GPRS'
,'HIGH_HOME_FLAG'
,'HOME_GPRS_RATE'
,'HOME_DEPEND'
,'ALL_ALL_GPRS'
,'SEARCH_DATE_ID'
,'AGE_FLAG1'
,'AGE_FLAG2'
,'STAR_LVL1'
,'STAR_LVL2'
,'STAR_LVL3'
,'P'
,'P_DAY'
,'LABEL'])
data[data.LABEL == 1]
data[data.LABEL == 0]