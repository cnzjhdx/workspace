--低通过滤器设计

--0.数据预处理，基础数据生成
create table jh_base_user as select * from sjwj_hive_db.l_bhv_cm_broadband_d a 
where a.p_day=20170507 and a.city_id=579 and a.p is not null;


--1.过滤生活在没有宽带资源的用户
drop table if exists tmp_jh_lpf_lac;
create table tmp_jh_lpf_lac as select a.* from jh_base_user a left join jh_lac_ci b on a.aft_lac_ci = b.lac_ci
where cnt is not null or a.p>0.5;

--2.过滤掉客户名称中含有“公司”的用户
drop table if exists tmp_jh_lpf_gc;
create table tmp_jh_lpf_gc as select a.* from tmp_jh_lpf_lac a 
where a.cust_name not like '%公司%';


--3.金华同宽带用户身份号码过滤
--3.1.取出金华宽带相关号码
drop table if exists jh_kd_user;
create table jh_kd_user as select * from sjwj_hive_db.l_bhv_cm_broadband_d a 
where a.p_day=20170507 and a.city_id=579 and a.p is null;

--3.2.增加证件号码
drop table if exists tmp_kd_cert;
create table tmp_kd_cert as select a.user_id,b.cert_code from jh_kd_user a left join
(select * from dwfu_hive_db.a_usoc_user_attr_d where b.p_day=20170507) b on a.user_id = b.user_id where 
b.cert_code is not null

--3.3.未办宽带号码增加身份证信息
drop table if exists tmp_jh_lpf_cert;
create table tmp_jh_lpf_cert as
select a.*,coalesce(b.cert_code,0) from tmp_jh_lpf_gc a left join
(select user_id,cert_code from dwfu_hive_db.a_usoc_user_attr_d b where b.p_day=20170507 ) b on a.user_id=b.user_id;

--3.4.身份证去重
drop table if exists tmp_kd_sfz;
create table tmp_kd_sfz as
select distinct cert_code from tmp_kd_cert;

--3.5剔除同省份证用户
create table tmp_jh_lpf_tysfz as
select * from tmp_jh_lpf_cert a where a.p< 0.5 and not exists (select * from tmp_kd_sfz b where a.cert_code =b.cert_code)
union all
select * from tmp_jh_lpf_cert a where a.p>=0.5

--4.家庭成员过滤







--5.过滤大学生




--数据过滤统计

select count(*) from jh_base_user;--5213355,其中73847条p>0.5
select count(*) from tmp_jh_lpf_lac;--4353266 过滤86万
select count(*) from tmp_jh_lpf_gc;--4137144 过滤22万
select count(*) from tmp_jh_lpf_tysfz;--3707393 过滤43万