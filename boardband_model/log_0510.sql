--1. 所有字段信息
/*USER_ID
ALL_ALL_GPRS
KD_RATE
AGE_RATE
CMCC_KD_CNT
HOME_DEPEND
SEARCH_BJ_FLAG
SEARCH_ZJ_FLAG
AGE_FLAG1
AGE_FLAG2
age_flag3
age_flag4
upay_flag
cluster2
cluster3
ARPU
ownserv_flag
otherserv_flag
stay_flag
LABEL*/




--2.看是否有空值
select count(*) USER_ID from jh_dataset t where  t.USER_ID  is null;
select count(*) ALL_ALL_GPRS from jh_dataset t where  t.ALL_ALL_GPRS  is null;
select count(*) KD_RATE from jh_dataset t where  t.KD_RATE  is null;
select count(*) AGE_RATE from jh_dataset t where  t.AGE_RATE  is null;
select count(*) CMCC_KD_CNT from jh_dataset t where  t.CMCC_KD_CNT  is null;
select count(*) HOME_DEPEND from jh_dataset t where  t.HOME_DEPEND  is null;
select count(*) SEARCH_BJ_FLAG from jh_dataset t where  t.SEARCH_BJ_FLAG  is null;
select count(*) SEARCH_ZJ_FLAG from jh_dataset t where  t.SEARCH_ZJ_FLAG  is null;
select count(*) AGE_FLAG1 from jh_dataset t where  t.AGE_FLAG1  is null;
select count(*) AGE_FLAG2 from jh_dataset t where  t.AGE_FLAG2  is null;
select count(*) age_flag3 from jh_dataset t where  t.age_flag3  is null;
select count(*) age_flag4 from jh_dataset t where  t.age_flag4  is null;
select count(*) upay_flag from jh_dataset t where  t.upay_flag  is null;
select count(*) cluster2 from jh_dataset t where  t.cluster2  is null;
select count(*) cluster3 from jh_dataset t where  t.cluster3  is null;
select count(*) ARPU from jh_dataset t where  t.ARPU  is null;
select count(*) ownserv_flag from jh_dataset t where  t.ownserv_flag  is null;
select count(*) otherserv_flag from jh_dataset t where  t.otherserv_flag  is null;
select count(*) stay_flag from jh_dataset t where  t.stay_flag  is null;
select count(*) LABEL from jh_dataset t where  t.LABEL  is null;


--3.算平均值
select avg(all_all_gprs) from jh_dataset;--1349.038168411447
select avg(KD_RATE) from jh_dataset;--0.16170072019204995
select avg(AGE_RATE) from jh_dataset;--0.05252147239263374
select avg(CMCC_KD_CNT) from jh_dataset;--1.5983120146264416
select avg(ARPU) from jh_dataset;--84.74734592804079


--4.空值填充


,coalesce(t.USER_ID,0)
,coalesce(t.ALL_ALL_GPRS,1349.04)
,coalesce(t.KD_RATE,0.16)
,coalesce(t.AGE_RATE,0.05)
,coalesce(t.CMCC_KD_CNT,1.60)
,coalesce(t.HOME_DEPEND,0)
,coalesce(t.SEARCH_BJ_FLAG,0)
,coalesce(t.SEARCH_ZJ_FLAG,0)
,coalesce(t.AGE_FLAG1,0)
,coalesce(t.AGE_FLAG2,0)
,coalesce(t.age_flag3,0)
,coalesce(t.age_flag4,0)
,coalesce(t.upay_flag,0)
,coalesce(t.cluster2,0)
,coalesce(t.cluster3,0)
,coalesce(t.ARPU,84.75)
,coalesce(t.ownserv_flag,0)
,coalesce(t.otherserv_flag,0)
,coalesce(t.stay_flag,0)
,coalesce(t.LABEL,0)


select count(*) from sjwj_hive_db.l_bhv_cm_broadband_d t where t.p_day=20170507 and t.city_id=579 and t.p is null and t.AFT_LAC_CI is not null;
select * from default.D_RNT_IRM_IV_ODP where p_day=20170503 and extensionid='36153906';
select * from sjwj_hive_db.temp_dht_grid_sys_data ;
select * from dwfu_hive_db.i_usoc_banduser_info_m where p_mon=201703 and city_id = 579;


 select  count(*) from  (select * from sjwj_hive_db.l_bhv_cm_broadband_d  where p_day='20170513') a left join jcc_broadban_data b on a.contact_user_id = b.user_id where b.contact_user_id is not null;
 
 select count(*) from jcc_broadban_data a left join (select * from sjwj_hive_db.l_bhv_cm_broadband_d  where p_day='20170513' and city_id=579) on a.contact_user_id = b.user_id where b.user_id is not null;



select a.*,b.fact_fee/100,b.gprs_fee/100,b.unpay_state
from jcc_result a left join (select * from dwfu_hive_db.a_upay_user_attr_m where p_mon =201703) a2 on a.user_id=a2.user_id




select a.* ,b. tmp_jcc_result a, (select * from dwfu_hive_db.i_tpos_stay_d t where p_day=20170430) b on a.product_no = b.bill_id






drop table if exists jcc_dataset;
create table jcc_dataset as select
coalesce(USER_ID,0) USER_ID
,coalesce(ALL_ALL_GPRS,1349.04) ALL_ALL_GPRS
,coalesce(KD_RATE,0.16) KD_RATE
,coalesce(AGE_RATE,0.05) AGE_RATE
,coalesce(CMCC_KD_CNT,1.60) CMCC_KD_CNT
,coalesce(HOME_DEPEND,0) HOME_DEPEND
,coalesce(SEARCH_BJ_FLAG,0) SEARCH_BJ_FLAG
,coalesce(SEARCH_ZJ_FLAG,0) SEARCH_ZJ_FLAG
,coalesce(AGE_FLAG1,0) AGE_FLAG1
,coalesce(AGE_FLAG2,0) AGE_FLAG2
,coalesce(age_flag3,0) age_flag3
,coalesce(age_flag4,0) age_flag4
,coalesce(upay_flag,0) upay_flag
,coalesce(cluster2,0) cluster2
,coalesce(cluster3,0) cluster3
,coalesce(ARPU,84.75) ARPU
,coalesce(ownserv_flag,0) ownserv_flag
,coalesce(otherserv_flag,0) otherserv_flag
,coalesce(stay_flag,0) stay_flag
, if (coalesce(innet_dur,10) <=3,1,0) new_user_flag
,coalesce(LABEL,0) LABEL
from tmp_jcc_result t

drop table if exists tmp_jcc_result;
create table tmp_jcc_result as
select a.*,
coalesce(a2.age1,0) age_flag3,
coalesce(a2.age4,0) age_flag4,
coalesce(a2.upay_flag,0) upay_flag,
coalesce(a2.stay_flag,0) stay_flag,
coalesce(a2.ownserv_flag,0) ownserv_flag,
coalesce(a2.otherserv_flag,0) otherserv_flag,
coalesce(a2.cluster2,0) cluster2,
coalesce(a2.cluster3,0) cluster3
from jcc_result a left join sjwj_hive_db.tmp_lmy_logit1_val a2 on a.user_id=a2.user_id















USER_ID
ALL_ALL_GPRS
KD_RATE
AGE_RATE
CMCC_KD_CNT
HOME_DEPEND
SEARCH_BJ_FLAG
SEARCH_ZJ_FLAG
AGE_FLAG1
AGE_FLAG2
age_flag3
age_flag4
upay_flag
cluster2
cluster3
ARPU
ownserv_flag
otherserv_flag
stay_flag
LABEL

coalesce(USER_ID,0) USER_ID
,coalesce(ALL_ALL_GPRS,1349.04) ALL_ALL_GPRS
,coalesce(KD_RATE,0.16) KD_RATE
,coalesce(AGE_RATE,0.05) AGE_RATE
,coalesce(CMCC_KD_CNT,1.60) CMCC_KD_CNT
,coalesce(HOME_DEPEND,0) HOME_DEPEND
,coalesce(SEARCH_BJ_FLAG,0) SEARCH_BJ_FLAG
,coalesce(SEARCH_ZJ_FLAG,0) SEARCH_ZJ_FLAG
,coalesce(AGE_FLAG1,0) AGE_FLAG1
,coalesce(AGE_FLAG2,0) AGE_FLAG2
,coalesce(age_flag3,0) age_flag3
,coalesce(age_flag4,0) age_flag4
,coalesce(upay_flag,0) upay_flag
,coalesce(cluster2,0) cluster2
,coalesce(cluster3,0) cluster3
,coalesce(ARPU,84.75) ARPU
,coalesce(ownserv_flag,0) ownserv_flag
,coalesce(otherserv_flag,0) otherserv_flag
,coalesce(stay_flag,0) stay_flag
,coalesce(LABEL,0) LABEL