--app ODS2.ODS_ti_c_pe_user_app_m_ms_202012


---   _20201231   2020-08-31   _202012    _202011  _202010

--8116907	8116907
drop table report2.report_5G_term_model_test_mid;
create table report2.report_5G_term_model_test_mid(
    user_id   varchar(20),
	phone_No  varchar(20),
	city_id   integer,
	county_id  varchar(20),
	create_date date,
	userstatus_id  integer,
	plan_id  varchar(20),
	cust_id   varchar(50),
--	age    varchar(10),
	sex_id  varchar(10),
	user_online  varchar(10),
	iden_no  varchar(50),
	user_class   varchar(10)
)WITH (OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid
select 
a.user_id
,a.phone_no
,a.city_id
,a.county_id
,a.create_date
,a.userstatus_id
,a.plan_id
,a.cust_id
--,2020-case when iden_id='1' and length(iden_no)=18 then substr(iden_no,7,4) end
,b.sex_id
,a.user_online
,b.iden_no
,e.credit_level
from dw2.DW_USER_BASE_INFO_ds_1_prt_p20201231 a 
left join ods2.ODS_prty_cust_info_ds_1_prt_p20210113 b on a.cust_id=b.cust_id
left join ods2.GP_INT_02004_02008_ds_1_prt_p20201231 d on a.user_id=d.user_id
left join ods2.GP_INT_M2M_USER_dt_1_prt_p20201231 c on a.user_id=c.user_id
left join (select user_id,max(credit_level) credit_level from ods2.ODS_am_user_credit_ds_1_prt_p20201231 group by user_id) e on a.user_id=e.user_id
where a.brand_id not in (6,7,11) and a.userstatus_id=1 and a.phone_no like '1%' and a.phone_no not like '10%' and c.user_id is null and sim_cust_id='0' and user_type_id<>'3' 
;




--select *
--from GP_INT_02004_02008_LAST_202012
--where user_type_id<>3 --测试卡  userstatus_id=1010  --状态正常  sim_cust_id=0  --剔除了数据卡和m2m业务用户
--GP_INT_M2M_USER_20201231 --物联网

--未更换5G当前终端以及机龄获取
--8077290	8077290
drop table report2.report_5G_term_model_test_mid1;
create table report2.report_5G_term_model_test_mid1(
    user_id   varchar(20),
	phone_No  varchar(20),
	city_id   varchar(10),
	county_id  varchar(20),
	create_date date,
	userstatus_id  varchar(10),
	plan_id  varchar(20),
	cust_id   varchar(50),
--	age    varchar(10),
	sex_id  varchar(10),
	user_online  varchar(10),
	iden_no  varchar(50),
	user_class   varchar(10),
	if_4G_cust   varchar(10),
	imei     varchar(20)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid1
select 
a.user_id
,a.phone_No
,a.city_id
,a.county_id
,a.create_date
,a.userstatus_id
,a.plan_id
,a.cust_id
--,a.age
,a.sex_id
,a.user_online
,a.iden_no
,a.user_class
,case when c1.user_id is not null then 1 else 0 end
,b.imei
from report2.report_5G_term_model_test_mid a 
left join dw2.DW_product_main_imei_ds_1_prt_p20201231 b on a.user_id=b.user_id
left join (select user_id
from report2.report_22208_4g_net_user_new_dt_1_prt_p20201231
where  flag_act=0) c1 on a.user_id=c1.user_id
left join dim2.DW_terminal_para_5g_tac c on b.tac = c.tac 
where c.tac is null;

drop table report2.report_boss_imei_cond_mid;
create table report2.report_boss_imei_cond_mid(
   imei_call  varchar(20),
   cond_id  varchar(20),
   busi_date  date
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(imei_call)                                                  
;
insert into report2.report_boss_imei_cond_mid
select a.imei_call,a.cond_id,a.busi_date
from (
select a.imei_call,a.cond_id,a.busi_date,row_number() over(partition by imei_call order by busi_date desc) as rk
from dw2.DW_mms139_terminal_dt_1_prt_p20210113 a 
left join (select imei from report2.report_5G_term_model_test_mid1 group by imei) b on a.imei_call=b.imei
where b.imei is not null)  a
where rk=1;






--机龄获取 8077290	8077290
drop table report2.report_5G_term_model_test_mid2;
create table report2.report_5G_term_model_test_mid2(
    user_id   varchar(20),
	phone_No  varchar(20),
	city_id   integer,
	county_id  varchar(20),
	create_date date,
	userstatus_id  integer,
	plan_id  varchar(20),
	cust_id   varchar(50),
--	age    integer,
	sex_id  integer,
	user_online  integer,
	iden_no  varchar(50),
	user_class   varchar(10),
	if_4G_cust   varchar(10),
	imei     varchar(20),
	ji_ling  integer,
	cond_id  varchar(20),
	busi_date  date,
	if_huanj   varchar(10)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid2
select
a.user_id
,a.phone_No
,a.city_id::int
,a.county_id::int
,a.create_date
,a.userstatus_id::int
,a.plan_id
,a.cust_id
--,a.age
,a.sex_id::int
,a.user_online::int
,a.iden_no
,a.user_class
,a.if_4G_cust
,a.imei
,timestampdiff ('MONTH', '2020-12-31 23:59:59' , to_char(b.op_time::timestamp,'yyyy-mm-dd hh24:mi:ss')) 
,c.cond_id
,c.busi_date
,case when c1.user_id is not null then 1 else 0 end
from report2.report_5G_term_model_test_mid1 a 
left join report2.report_boss_imei_cond_mid c on a.imei=c.imei_call
left join (select imei,min(op_time) op_time from dw2.DW_TD_INTELLIGENCE_MACHINE_YWJ_MID2 group by imei) b on a.imei=b.imei
left join (select distinct user_id from dw2.DW_TD_INTELLIGENCE_MACHINE_YWJ_MID2 where op_time>='2020-02-01' and op_time<='2020-12-31') c1 on a.user_id=c1.user_id;


--为换机终端合约到期时间
--8077290	8077290
drop table report2.report_5G_term_model_test_mid3;
create table report2.report_5G_term_model_test_mid3(
    user_id   varchar(20),
	phone_No  varchar(20),
	city_id   integer,
	county_id  varchar(20),
	create_date date,
	userstatus_id  integer,
	plan_id  varchar(20),
	cust_id   varchar(50),
--	age    integer,
	sex_id  integer,
	user_online  integer,
	iden_no  varchar(50),
	user_class   varchar(10),
	if_4G_cust   varchar(10),
	imei     varchar(20),
	ji_ling  integer,
	cond_id  varchar(20),
	busi_date  date,	
	if_huanj   varchar(10),
	expire_date  date
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid3
select                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
a.user_id
,a.phone_No
,a.city_id
,a.county_id
,a.create_date
,a.userstatus_id
,a.plan_id
,a.cust_id
--,a.age
,a.sex_id
,a.user_online
,a.iden_no
,a.user_class
,a.if_4G_cust
,a.imei
,a.ji_ling
,a.cond_id  
,a.busi_date 
,a.if_huanj                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
,date(VALUE(b.expire_date::date,b1.expire_date::date))                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
from report2.report_5G_term_model_test_mid2 a                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
left join (select substr(imei_id,1,14) imei_id,max(expire_date) expire_date
from ODS2.ODS_I_DEPOSIT_ALLOT_DS_1_prt_p20201231
group by substr(imei_id,1,14)) b on  a.imei=b.imei_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
left join (select substr(imei_id,1,14) imei_id,max(expire_date) expire_date
from ODS2.ODS_I_DEPOSIT_ALLOT_H_DS_1_prt_p20201231
group by substr(imei_id,1,14)) b1 on  a.imei=b1.imei_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
;          



--统一身份证下号码数
drop table report2.report_5G_term_model_test1_mid1;
create table report2.report_5G_term_model_test1_mid1(
   iden_no    varchar(50),
   num     integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(iden_no)                                                  
;
insert into report2.report_5G_term_model_test1_mid1
select iden_no,count(distinct phone_no)
from dw2.DW_USER_BASE_INFO_ds_1_prt_p20201231 a 
left join ODS2.ODS_prty_cust_info_ds_1_prt_p20210113 b on a.cust_id=b.cust_id
where brand_id not in (6,7,11) and userstatus_id=1 and phone_no like '1%' and phone_no not like '10%'
group by iden_no;



--目标用户获取(未更换5G，剔除机龄<=6，剔除终端合约到期时间大于两个月的用户) 4090645	4090645

drop table report2.report_5G_term_model_test_mid4;
create table report2.report_5G_term_model_test_mid4(
    user_id   varchar(20),
	phone_No  varchar(20),
	city_id   integer,
	county_id  varchar(20),
	create_date date,
	userstatus_id  integer,
	plan_id  varchar(20),
	cust_id   varchar(50),
--	age    integer,
	sex_id  integer,
	user_online  integer,
	iden_no  varchar(50),
	user_class   varchar(10),
	if_4G_cust   varchar(10),
    iden_num  integer,
	imei     varchar(20),
	ji_ling  integer,
	cond_id  varchar(20),
	busi_date  date,
	if_huanj   varchar(10),
	expire_date  date
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid4
select
a.user_id
,a.phone_No
,a.city_id
,a.county_id
,a.create_date
,a.userstatus_id
,a.plan_id
,a.cust_id
--,a.age
,a.sex_id
,a.user_online
,a.iden_no
,a.user_class
,a.if_4G_cust
,value(b.num,0)
,a.imei
,a.ji_ling 
,a.cond_id  
,a.busi_date  
,a.if_huanj                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
,a.expire_date  
from report2.report_5G_term_model_test_mid3 a 
left join report2.report_5G_term_model_test1_mid1 b on a.iden_no=b.iden_no
where ji_ling>6 and (expire_date is null or expire_date<='2021-02-28'); ---剔除终端合约到期时间大于两个月的用户

--年龄获取
drop table report2.report_user_age_huoqu_mid;
create table report2.report_user_age_huoqu_mid(
    user_id   varchar(20),
	iden_no  varchar(50),
	age    integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(user_id) 
;
insert into report2.report_user_age_huoqu_mid
select a.user_id,a.iden_no
,2021-case when b.iden_id='1' and length(a.iden_no)=18 then substr(a.iden_no,7,4)::int when iden_id='1' and length(a.iden_no)=18 then ('19'||substr(a.iden_no,7,2))::int end age
from report2.report_5G_term_model_test_mid4 a 
left join ODS2.ODS_prty_cust_info_ds_1_prt_p20210113 b on a.cust_id=b.cust_id;


--终端使用情况 4090645	4090645

drop table report2.report_5G_term_model_test_mid5;
create table report2.report_5G_term_model_test_mid5(
    user_id   varchar(20),
	phone_No  varchar(20),
	city_id   integer,
	county_id  varchar(20),
	create_date date,
	userstatus_id  integer,
	plan_id  varchar(20),
	cust_id   varchar(50),
	age    integer,
	sex_id  integer,
	user_online  integer,
	iden_no  varchar(50),
	user_class   varchar(10),
	if_4G_cust   varchar(10),
    iden_num  integer,
	imei     varchar(20),
	ji_ling  integer,
	cond_id  varchar(20),
	busi_date  date,
	if_huanj   varchar(10),
	expire_date  date,
	termbrand_name  varchar(50),
	price   integer,
	price_fd  varchar(20)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid5
select
a.user_id
,a.phone_No
,a.city_id
,a.county_id
,a.create_date
,a.userstatus_id
,a.plan_id
,a.cust_id
,d.age
,a.sex_id
,a.user_online
,a.iden_no
,a.user_class
,a.if_4G_cust
,a.iden_num
,a.imei
,a.ji_ling
,a.cond_id  
,a.busi_date 
,a.if_huanj
,a.expire_date
,b.termbrand_name
,value(c.price,0)
,case when value(c.price,0)>0 and value(c.price,0)<=800 then '(0,800]'
      when value(c.price,0)>800 and value(c.price,0)<=1800 then '(800,1800]'
	  when value(c.price,0)>1800 and value(c.price,0)<=3000 then '(1800,3000]'
	  when value(c.price,0)>3000 and value(c.price,0)<=4000 then '(3000,4000]'
	   when value(c.price,0)>4000 and value(c.price,0)<=5000 then '(4000,5000]'
	   when value(c.price,0)>5000  then '(5000,++]' end
from report2.report_5G_term_model_test_mid4 a 
left join report2.report_user_age_huoqu_mid d on a.user_id=d.user_id
left join dw2.DW_IMEI_TERMINFO_DS_1_prt_p20210113 b on substr(a.imei,1,8) = b.tac 
left join dim2.report_yml_4gtac_price c on substr(a.imei,1,8)=c.tac;

--是否双卡槽

/*
drop table report2.report_shuangimei_test_mid;
create table  report2.report_shuangimei_test_mid(
		new_cg       varchar(50) ,
		old_cg       varchar(50)
)distribute by hash(old_cg) in tbs_crm_app not logged initially;
insert into report2.report_shuangimei_test_mid
select substr(new_cg,1,14) new_cg,max(substr(old_cg,1,14)) old_cg from jxcrm.ODS2.ODS_shuangimei_dm group by substr(new_cg,1,14);


drop table report2.report_shuangimei_test_mid1;
create table  report2.report_shuangimei_test_mid1(
		old_cg       varchar(20) ,
		new_cg       varchar(20)
)distribute by hash(old_cg) in tbs_crm_app not logged initially;
insert into report2.report_shuangimei_test_mid1
select substr(old_cg,1,14) old_cg,max(substr(new_cg,1,14)) new_cg from jxcrm.ODS2.ODS_shuangimei_dm group by substr(old_cg,1,14);


drop table report2.report_shuangimei_test_mid2;
create table report2.report_shuangimei_test_mid2(
    imei   varchar(20),
    imei_other  varchar(20)
)distribute by hash(imei) in tbs_crm_app not logged initially;
insert into report2.report_shuangimei_test_mid2
select a.imei,value(b.new_cg,c.new_cg)
from report2.report_5G_term_model_test_mid5 a 
left join (select new_cg , max(old_cg) old_cg from report2.report_shuangimei_test_mid group by new_cg) b on a.imei=b.new_cg
left join (select old_cg , max(new_cg) new_cg from report2.report_shuangimei_test_mid1 group by old_cg) c on a.imei=c.old_cg;
*/



--是否是4G终端，是否合约终端 4090645	4090645

drop table report2.report_5G_term_model_test_mid6;
create table report2.report_5G_term_model_test_mid6(
    user_id   varchar(20),
	phone_No  varchar(20),
	city_id   integer,
	county_id  varchar(20),
	create_date date,
	userstatus_id  integer,
	plan_id  varchar(20),
	cust_id   varchar(50),
	age    integer,
	sex_id  integer,
	user_online  integer,
	iden_no  varchar(50),
	user_class   varchar(10),
	if_4G_cust   varchar(10),
    iden_num  integer,
	imei     varchar(20),
	ji_ling  integer,
	cond_id  varchar(20),
	busi_date  date,
	if_huanj   varchar(10),
	expire_date  date,
	termbrand_name  varchar(50),
	price   integer,
	price_fd  varchar(20),
	low_cost   decimal(20,2),
	if_hyj  integer,
	if_skc  varchar(20),
	if_4G_term   integer,
	expire_month  varchar(10)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid6
select
a.user_id
,a.phone_No
,a.city_id
,a.county_id
,a.create_date
,a.userstatus_id
,a.plan_id
,a.cust_id
,a.age
,a.sex_id
,a.user_online
,a.iden_no
,a.user_class
,a.if_4G_cust
,a.iden_num
,a.imei
,a.ji_ling
,a.cond_id  
,a.busi_date 
,a.if_huanj
,a.expire_date
,a.termbrand_name
,a.price
,a.price_fd
,e.low_cost*0.01
,case when a.cond_id is not null then 1 else 0 end
,case when d.tac is not null then 1 else 0 end
,case when b.netstyle_flag=8 then 1 else 0 end
,case when a.expire_date>'2020-12-31' then timestampdiff ('MONTH', a.expire_date ,'2020-12-31') else null end
from report2.report_5G_term_model_test_mid5 a 
left join dim2.report_YML_SKJ_TAC d on substr(a.imei,1,8)=d.tac
left join dw2.DW_IMEI_TERMINFO_DS_1_prt_p20210113 b on substr(a.imei,1,8)=b.tac
left join ODS2.ODS_up_prepay_ds_1_prt_p20210113 e on a.cond_id=e.prepay_id;


drop table report2.report_user_flow_bhd_mid;
create table report2.report_user_flow_bhd_mid(
   user_id  varchar(20),
   flow_unit  decimal(20,2),
   flow_bhd1  decimal(20,2),
   flow_bhd2  decimal(20,2),
   flow_bhd3  decimal(20,2),
   arpu1       decimal(20,2),
   arpu2       decimal(20,2),
   arpu3       decimal(20,2),
   flow1       decimal(20,2),
   flow2       decimal(20,2),
   flow3       decimal(20,2),
   avg_ctc_flow_fee  decimal(20,2)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(user_id) 
;
insert into report2.report_user_flow_bhd_mid
select a.user_id
,b.flow_unit
,b.flow_bhd
,c.flow_bhd
,d.flow_bhd
,b.user_consume
,c.user_consume
,d.user_consume
,b.flow
,c.flow
,d.flow
,(value(b.ctc_flow_fee,0)+value(c.ctc_flow_fee,0)+value(d.ctc_flow_fee,0))*1.0/3 
from report2.report_5G_term_model_test_mid6 a 
left join ods2.dmsa_DW_user_fee_ms_1_prt_p202012 b on a.user_id=b.user_id 
left join ods2.dmsa_DW_user_fee_ms_1_prt_p202011 c on a.user_id=c.user_id
left join ods2.dmsa_DW_user_fee_ms_1_prt_p202010 d on a.user_id=d.user_id;


--上月23G流量占比
drop table report2.report_flow_avg_zb_mid;
create table report2.report_flow_avg_zb_mid(
   user_id  varchar(20),
   flow_23_zb1   decimal(20,2),
   flow_23_zb2  decimal(20,2),
   flow_23_zb3   decimal(20,2)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(user_id) 
;
insert into report2.report_flow_avg_zb_mid
select a.user_id
,case when b.flow*1.0/1024/1024=0 then 0 else (b.flow_2G+b.flow_3G)*1.0000/b.flow*1.0/1024/1024 end
,case when c.flow*1.0/1024/1024=0 then 0 else (c.flow_2G+c.flow_3G)*1.0000/c.flow*1.0/1024/1024 end
,case when d.flow*1.0/1024/1024=0 then 0 else (d.flow_2G+d.flow_3G)*1.0000/d.flow*1.0/1024/1024 end
from report2.report_5G_term_model_test_mid6 a 
left join dw2.stat_product_gprs_1_prt_p202012 b on a.user_id=b.user_id
left join dw2.stat_product_gprs_1_prt_p202011 c on a.user_id=c.user_id 
left join dw2.stat_product_gprs_1_prt_p202010 d on a.user_id=d.user_id;



--消费情况 4090645	4090645
drop table report2.report_5G_term_model_test_mid7;
create table report2.report_5G_term_model_test_mid7(
    user_id   varchar(20),
	phone_No  varchar(20),
	city_id   integer,
	county_id  varchar(20),
	create_date date,
	userstatus_id  integer,
	plan_id  varchar(20),
	cust_id   varchar(50),
	age    integer,
	sex_id  integer,
	user_online  integer,
	iden_no  varchar(50),
	user_class   varchar(10),
	if_4G_cust   varchar(10),
    iden_num  integer,
	imei     varchar(20),
	ji_ling  integer,
	cond_id  varchar(20),
	busi_date  date,
	if_huanj   varchar(10),
	expire_date  date,
	termbrand_name  varchar(50),
	price   integer,
	price_fd  varchar(20),
	low_cost   decimal(20,2),
	if_hyj  integer,
	if_skc  varchar(20),
	if_4G_term   integer,
	expire_month  varchar(10),
	
	plan_zifei   integer,
	flow_unit  decimal(20,2),
	if_dll  integer,
	
   arpu1       decimal(20,2),
   arpu2       decimal(20,2),
   arpu3       decimal(20,2),
   flow1       decimal(20,2),
   flow2       decimal(20,2),
   flow3       decimal(20,2),
   flow_bhd1  decimal(20,2),
   flow_bhd2  decimal(20,2),
   flow_bhd3  decimal(20,2),
   avg_ctc_flow_fee   decimal(20,2),
   flow_23_zb    decimal(20,2),
   flow_23_zb_3  decimal(20,2)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid7
select
a.user_id
,a.phone_No
,a.city_id
,a.county_id
,a.create_date
,a.userstatus_id
,a.plan_id
,a.cust_id
,a.age
,a.sex_id
,a.user_online
,a.iden_no
,a.user_class
,a.if_4G_cust
,a.iden_num
,a.imei
,a.ji_ling
,a.cond_id  
,a.busi_date
,a.if_huanj 
,a.expire_date
,a.termbrand_name
,a.price
,a.price_fd
,a.low_cost
,a.if_hyj
,a.if_skc
,a.if_4G_term 
,a.expire_month

,b.plan_zifei
,c.flow_unit
,case when b.flag_bxl=1 then 1 else 0 end

,c.arpu1       
,c.arpu2       
,c.arpu3       
,c.flow1       
,c.flow2       
,c.flow3
,c.flow_bhd1  
,c.flow_bhd2  
,c.flow_bhd3 
      
,c.avg_ctc_flow_fee 

,d.flow_23_zb1  
,(d.flow_23_zb1+d.flow_23_zb2+d.flow_23_zb3)*1.0/3
from report2.report_5G_term_model_test_mid6 a 
left join report2.report_user_flow_bhd_mid c on a.user_id=c.user_id
left join report2.report_flow_avg_zb_mid d on a.user_id=d.user_id
left join dim2.report_plan_fenlei_v15_new b on a.plan_id=b.plan_id;


--上网偏好
drop table report2.report_app_flow_user_mid;
create table report2.report_app_flow_user_mid(
   serv_no   varchar(20),
   app_cat_name  varchar(20),
   flow    decimal(20,2)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(serv_no)                                                  
;
insert into report2.report_app_flow_user_mid
select serv_no,app_cat_name,sum(sum_all_flux)
from ODS2.ODS_ti_c_pe_user_app_m_ms_1_prt_p202012 a 
left join report2.report_5G_term_model_test_mid7 b on a.serv_no=b.phone_No
where app_cat_name in ('视频','游戏','音乐') and b.phone_No is not null
group by serv_no,app_cat_name;


drop table report2.report_5G_term_model_test_mid8;
create table report2.report_5G_term_model_test_mid8(
   user_id   varchar(20),       --用户编号
	phone_No  varchar(20),     --号码
	city_id   integer,        --地市
	county_id  varchar(20),     --办理区县
	create_date date,           --办理时间
	userstatus_id  integer,     --用户状态
	plan_id  varchar(20),     --服务计划
	cust_id   varchar(50),    --客户编号
	age    integer,        --年龄
	sex_id  integer,        --性别
	user_online  integer,     --网龄
	iden_no  varchar(50),      --证件号
	user_class   varchar(10),
	if_4G_cust   varchar(10),
    iden_num  integer,        --该证件下号码数
	imei     varchar(20),     --串号
	ji_ling  integer,        --机龄
	cond_id  varchar(20),     --押金
	busi_date  date,            --终端合约办理时间
	if_huanj   varchar(10),
	expire_date  date,       --到期时间
	termbrand_name  varchar(50),    --当前终端品牌
	price   integer,          --当前终端价格 
	price_fd  varchar(20),    --当前终端价格分档
	low_cost   decimal(20,2),  --合约最低消费
	if_hyj  integer,        --是否是合约机
	if_skc  varchar(20),    --是否双卡槽
	if_4G_term   integer,    --是否是4G终端
	expire_month  varchar(10),   --当前终端合约剩余期限
	
	plan_zifei   integer,       --套餐费
	flow_unit  decimal(20,2),   --套内流量
	if_dll  integer,            --是否大流量
	
   arpu1       decimal(20,2),    --T-1ARPU
   arpu2       decimal(20,2),    --T-2ARPU
   arpu3       decimal(20,2),    --T-3ARPU
   flow1       decimal(20,2),    --T-1DOU
   flow2       decimal(20,2),    --T-2DOU
   flow3       decimal(20,2),    --T-3DOU
   flow_bhd1  decimal(20,2),     --T-1饱和度
   flow_bhd2  decimal(20,2),     --T-2饱和度
   flow_bhd3  decimal(20,2),     --T-3饱和度
   avg_ctc_flow_fee   decimal(20,2),   --近3个月平均饱和度
   flow_23_zb    decimal(20,2),       --23G流量占比
   flow_23_zb_3  decimal(20,2),       --近3个月23G流量占比
   
   flow_yx    decimal(20,2),          --游戏流量
   flow_yy    decimal(20,2),          --语音流量
   flow_sp    decimal(20,2),         --视频流量
   flow_yx_zb   decimal(20,2),        --游戏流量占比
   flow_yy_zb   decimal(20,2),        --语音流量占比
   flow_sp_zb   decimal(20,2)         --视频流量占比
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid8
select 
a.user_id
,a.phone_No
,a.city_id
,a.county_id
,a.create_date
,a.userstatus_id
,a.plan_id
,a.cust_id
,a.age
,a.sex_id
,a.user_online
,a.iden_no
,a.user_class
,a.if_4G_cust
,a.iden_num
,a.imei
,a.ji_ling
,a.cond_id
,a.busi_date
,a.if_huanj
,a.expire_date
,a.termbrand_name
,a.price
,a.price_fd
,a.low_cost
,a.if_hyj
,a.if_skc
,a.if_4G_term
,a.expire_month
,a.plan_zifei
,a.flow_unit
,a.if_dll
,a.arpu1
,a.arpu2
,a.arpu3
,a.flow1
,a.flow2
,a.flow3
,a.flow_bhd1
,a.flow_bhd2
,a.flow_bhd3
,a.avg_ctc_flow_fee
,a.flow_23_zb
,a.flow_23_zb_3
,b.flow
,c.flow
,d.flow
,case when value(a.flow1,0)=0 then 0 else b.flow*1.0/a.flow1 end
,case when value(a.flow1,0)=0 then 0 else c.flow*1.0/a.flow1 end
,case when value(a.flow1,0)=0 then 0 else d.flow*1.0/a.flow1 end
from report2.report_5G_term_model_test_mid7 a 
left join (select serv_no,flow from report2.report_app_flow_user_mid where app_cat_name='游戏') b on a.phone_no=b.serv_no
left join (select serv_no,flow from report2.report_app_flow_user_mid where app_cat_name='音乐') c on a.phone_no=c.serv_no
left join (select serv_no,flow from report2.report_app_flow_user_mid where app_cat_name='视频') d on a.phone_no=d.serv_no;


--客户类型  --15244932

drop table report2.report_5G_term_model_test_mid9;
create table report2.report_5G_term_model_test_mid9(
    user_id   varchar(20),       --用户编号
	phone_No  varchar(20),     --号码
	city_id   integer,        --地市
	county_id  varchar(20),     --办理区县
	create_date date,           --办理时间
	userstatus_id  integer,     --用户状态
	plan_id  varchar(20),     --服务计划
	cust_id   varchar(50),    --客户编号
	age    integer,        --年龄
	sex_id  integer,        --性别
	user_online  integer,     --网龄
	cust_type    varchar(10),   --客户类型1：个人，2：集团，3校园
	city_type   varchar(10),  ----城县农标志，1：城市，2：县城，3：农村，9：其他
	iden_no  varchar(50),      --证件号
	user_class   varchar(10),
	if_4G_cust   varchar(10),
    iden_num  integer,        --该证件下号码数
	if_qqt   integer,  --是否全球通
	if_jttf   integer,--是否集团统付
	imei     varchar(20),     --串号
	ji_ling  integer,        --机龄
	cond_id  varchar(20),     --押金
	busi_date  date,            --终端合约办理时间
	if_huanj   varchar(10),
	expire_date  date,       --到期时间
	termbrand_name  varchar(50),    --当前终端品牌
	price   integer,          --当前终端价格 
	price_fd  varchar(20),    --当前终端价格分档
	low_cost   decimal(20,2),  --合约最低消费
	if_hyj  integer,        --是否是合约机
	if_skc  varchar(20),    --是否双卡槽
	if_4G_term   integer,    --是否是4G终端
	expire_month  varchar(10),   --当前终端合约剩余期限
	
	plan_zifei   integer,       --套餐费
	flow_unit  decimal(20,2),   --套内流量
	if_dll  integer,            --是否大流量
	
   arpu1       decimal(20,2),    --T-1ARPU
   arpu2       decimal(20,2),    --T-2ARPU
   arpu3       decimal(20,2),    --T-3ARPU
   flow1       decimal(20,2),    --T-1DOU
   flow2       decimal(20,2),    --T-2DOU
   flow3       decimal(20,2),    --T-3DOU
   flow_bhd1  decimal(20,2),     --T-1饱和度
   flow_bhd2  decimal(20,2),     --T-2饱和度
   flow_bhd3  decimal(20,2),     --T-3饱和度
   avg_ctc_flow_fee   decimal(20,2),   --近3个月平均饱和度
   flow_23_zb    decimal(20,2),       --23G流量占比
   flow_23_zb_3  decimal(20,2),       --近3个月23G流量占比
   
   flow_yx    decimal(20,2),          --游戏流量
   flow_yy    decimal(20,2),          --语音流量
   flow_sp    decimal(20,2),         --视频流量
   flow_yx_zb   decimal(20,2),        --游戏流量占比
   flow_yy_zb   decimal(20,2),        --语音流量占比
   flow_sp_zb   decimal(20,2)         --视频流量占比
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid9
select 
a.user_id--用户编号
,a.phone_No--号码
,a.city_id  --地市
,a.county_id--办理区县
,a.create_date--办理时间
,a.userstatus_id --用户状态
,a.plan_id--服务计划
,a.cust_id--客户编号
,a.age--年龄
,a.sex_id --性别
,a.user_online --网龄
,case when b.serv_no is not null then 2 
when c.product_no is not null then 3 else 1 end  --客户类型
,case when f.region_type=1 then 1 
when f.region_type=2 then 2 
when f.region_type=3 then 3 else 9 end --城县农标志，1：城市，2：县城，3：农村，9：其他
,a.iden_no --证件号
,a.user_class
,a.if_4G_cust
,a. iden_num --该证件下号码数
,case when e.phone_no is not null then 1 else 0 end
,case when d.product_no is not null then 1 else 0 end
,a.imei--串号
,a.ji_ling --机龄
,a.cond_id--押金
,a.busi_date  --终端合约办理时间
,a.if_huanj
,a.expire_date--到期时间
,a.termbrand_name  --当前终端品牌
,a.price --当前终端价格 
,a.price_fd  --当前终端价格分档
,a.low_cost  --合约最低消费
,a.if_hyj --是否是合约机
,a.if_skc  --是否双卡槽
,a.if_4G_term --是否是4G终端
,a.expire_month  --当前终端合约剩余期限
,a.plan_zifei --套餐费
,a.flow_unit  --套内流量
,a.if_dll  --是否大流量
,a.arpu1  --T-1ARPU
,a.arpu2  --T-2ARPU
,a.arpu3  --T-3ARPU
,a.flow1  --T-1DOU
,a.flow2  --T-2DOU
,a.flow3  --T-3DOU
,a.flow_bhd1 --T-1饱和度
,a.flow_bhd2 --T-2饱和度
,a.flow_bhd3 --T-3饱和度
,a.avg_ctc_flow_fee--近3个月平均饱和度
,a.flow_23_zb  --23G流量占比
,a.flow_23_zb_3--近3个月23G流量占比
,a.flow_yx  --游戏流量
,a.flow_yy  --语音流量
,a.flow_sp --视频流量
,a.flow_yx_zb  --游戏流量占比
,a.flow_yy_zb  --语音流量占比
,a.flow_sp_zb--视频流量占比
from report2.report_5G_term_model_test_mid8 a 
left join (select distinct serv_no from ODS2.ODS_cm_group_archives_number_ds_1_prt_p20210113 ) b on a.phone_no=b.serv_no
left join (select distinct product_no from dw2.dmsch_dw_product_student_identify_dt_1_prt_p20210113 where status_id <> 4) c on a.phone_no=c.product_no
left join (select distinct product_no from report2.stat_group_up_unionpay_mid2_mm_1_prt_p202012) d on a.phone_no=d.product_no
left join (select distinct phone_no from dw2.DW_gsm_user_level_detail_yyyymmdd_1_prt_p20201231) e on a.phone_no=e.phone_no
left join dw2.DW_regioncust_ds_1_prt_p20201231 f on a.user_id=f.user_id;

--家庭交网全是否有5G
drop table report2.report_family_5Gzd_mid11;
create table report2.report_family_5Gzd_mid11(
   serv_id   varchar(20),
   offer_inst_id  varchar(20)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(serv_id)                                                  
;
insert into report2.report_family_5Gzd_mid11
select serv_id,offer_inst_id
from (
	select 
		a.serv_id,a.offer_inst_id,row_number() over(partition by serv_id order by create_date desc) as rk 
	from dw2.DW_cm_family_member_1_prt_p20201231 a 
	left join (
		select distinct offer_inst_id
		from (
			select serv_id,offer_inst_id
			from (
				select serv_id,offer_inst_id,row_number() over(partition by serv_id order by create_date desc) as rk  
					from dw2.DW_cm_family_member_1_prt_p20201231 a
			)  a
			where rk=1 
	) a 
	left join report2.report_5G_term_model_test_mid9 b on a.serv_id=b.user_id 
	where b.user_id is not null
) b on a.offer_inst_id=b.offer_inst_id
where b.offer_inst_id is not null
) a
where rk=1;



drop table report2.report_family_5Gzd_mid;
create table report2.report_family_5Gzd_mid(
    serv_id   varchar(20),
	offer_inst_id  varchar(20),
	if_5G    integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(serv_id)                                                  
;
insert into report2.report_family_5Gzd_mid
select a.serv_id,a.offer_inst_id,case when b.user_id is not null then 1 else 0 end
from report2.report_family_5Gzd_mid11 a 
left join (select user_id
from dw2.DW_product_main_imei_ds_1_prt_p20201231 a 
left join dim2.DW_terminal_para_5g_tac b on a.tac=b.tac 
where b.tac is not null) b on a.serv_id=b.user_id;



drop table report2.report_family_5Gzd_mid1;
create table report2.report_family_5Gzd_mid1(
  offer_inst_id   varchar(20),
  num    integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(offer_inst_id) 
;
select offer_inst_id,sum(if_5G)
from report2.report_family_5Gzd_mid
group by offer_inst_id;

drop table report2.report_family_5Gzd_mid12;
create table report2.report_family_5Gzd_mid12(
    user_id   varchar(20),
	offer_inst_id  varchar(20),
	if_5G   INTEGER
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(offer_inst_id) 
;
insert into report2.report_family_5Gzd_mid12
select a.serv_id,a.offer_inst_id,case when b.num>0 then 1 else 0 end as num
from report2.report_family_5Gzd_mid a 
left join report2.report_family_5Gzd_mid1 b on a.offer_inst_id = b.offer_inst_id;






drop table report2.report_user_group_seq_mid;
create table report2.report_user_group_seq_mid(
bill_id  varchar(40),
   group_seq   varchar(40)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(bill_id) 
;
insert into report2.report_user_group_seq_mid
select serv_no,group_seq
from (
select serv_no,group_seq,row_number() over(partition by serv_no order by create_date desc) as rk  
from ODS2.ODS_cm_group_archives_number_ds_1_prt_p20201231)  a
where rk=1;



drop table report2.report_user_group_seq12_mid;
create table report2.report_user_group_seq12_mid(
   group_seq   varchar(40)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(group_seq) 
;
insert into report2.report_user_group_seq12_mid
select distinct group_seq from report2.report_user_group_seq_mid a 
left join report2.report_5G_term_model_test_mid9 b on a.bill_id=b.phone_no
where b.phone_no is not null;

--集团交网全是否有5G
drop table report2.report_jituan_5G_mid11;
create table report2.report_jituan_5G_mid11(
   bill_id   varchar(40),
   group_seq  varchar(30)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(bill_id) 
;
insert into report2.report_jituan_5G_mid11
select a.serv_no,a.group_seq
from (
select a.serv_no,a.group_seq,row_number() over(partition by serv_no order by create_date desc) as rk 
from ODS2.ODS_cm_group_archives_number_ds_1_prt_p20201231 a 
left join (select distinct group_seq from report2.report_user_group_seq12_mid) b on a.group_seq=b.group_seq
where b.group_seq is not null) a
where rk=1;




drop table report2.report_jituan_5G_mid;
create table report2.report_jituan_5G_mid(
   bill_id  varchar(40),
   group_seq  varchar(40),
   if_5G    integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(bill_id) 
;
insert into report2.report_jituan_5G_mid
select a.bill_id,a.group_seq,case when b.product_no is not null then 1 else 0 end
from report2.report_jituan_5G_mid11 a 
left join (select product_no
from dw2.DW_product_main_imei_ds_1_prt_p20201231 a 
left join dim2.DW_terminal_para_5g_tac b on a.tac=b.tac 
where b.tac is not null) b on a.bill_id = b.product_no;

drop table report2.report_jituan_5G_mid1;
create table report2.report_jituan_5G_mid1(
  group_seq   varchar(40),
  num    integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(group_seq) 
;
insert into report2.report_jituan_5G_mid1
select group_seq,sum(if_5G)
from report2.report_jituan_5G_mid
group by group_seq;


drop table report2.report_jituan_5G_mid12;
create table report2.report_jituan_5G_mid12(
     bill_id   varchar(50),
	 group_seq  varchar(50),
	 if_5G   integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(bill_id) 
;
insert into report2.report_jituan_5G_mid12
select a.bill_id,a.group_seq,case when b.num>0 then 1 else 0 end
from report2.report_jituan_5G_mid a 
left join report2.report_jituan_5G_mid1 b on a.group_seq=b.group_seq;







--换机次数，换机时长
drop table report2.report_user_huanji_total_mid;
create table report2.report_user_huanji_total_mid(
   user_id  varchar(20),
   imei    varchar(20),
   op_time   date,
   rk      integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(user_id) 
;
insert into report2.report_user_huanji_total_mid
select user_id,imei,op_time,rk
from (
select a.user_id,a.imei,a.op_time,row_number() over(partition by a.user_id order by op_time desc) as rk
from dw2.DW_TD_INTELLIGENCE_MACHINE_YWJ_MID2 a 
left join (select user_id from report2.report_5G_term_model_test_mid9) b on a.user_id=b.user_id 
where b.user_id is not null) a ;

--换机时长
drop table report2.report_user_huanji_total_mid1;
create table report2.report_user_huanji_total_mid1(
    user_id  varchar(20),
	imei    varchar(20),
	huanj_month  integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(user_id) 
;
insert into report2.report_user_huanji_total_mid1
select a.user_id,a.imei, case when b.user_id is not null then timestampdiff ('month', a.op_time,b.op_time) else null end --999表示未换机
from (
select user_id,imei,op_time
from report2.report_user_huanji_total_mid
where rk=1) a 
left join (select user_id,imei,op_time
from report2.report_user_huanji_total_mid
where rk=2) b on a.user_id=b.user_id;

--上次终端品牌，上次使用时长
drop table report2.report_scond_time_term_mid;
create table report2.report_scond_time_term_mid(
    user_id   varchar(20),
	imei      varchar(20),
	termbrand_name  varchar(50),
	op_time  date
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(user_id) 
;
insert into report2.report_scond_time_term_mid
select a.user_id,a.imei,b.termbrand_name,a.op_time
from report2.report_user_huanji_total_mid a 
left join dw2.DW_IMEI_TERMINFO_DS_1_prt_p20210113 b on substr(a.imei,1,8)=b.tac
where rk=2;
--近3年换机次数,历史换机时长
drop table report2.report_latest_3_huanji_mid;
create table report2.report_latest_3_huanji_mid(
    user_id  varchar(20),
	num     integer,
	avg_hj  decimal(20,2)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(user_id) 
;
insert into report2.report_latest_3_huanji_mid
select user_id,count(1),36/count(1)
from report2.report_user_huanji_total_mid
where op_time>='2017-12-01' and op_time<='2020-12-31'
group by user_id;


--近3月月均通话天数，流量天数
drop table report2.report_call_gprs_days_mid;
create table report2.report_call_gprs_days_mid(
   user_id  varchar(20),
   call_days_avg  integer,
   gprs_days_avg   integer
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(user_id) 
;
insert into report2.report_call_gprs_days_mid
select a.user_id
,case when b.user_id is not null and c.user_id is not null and d.user_id is not null 
then (value(b.call_days,0)+value(c.call_days,0)+value(d.call_days,0))*1.0/3 
when b.user_id is not null and c.user_id is not null and d.user_id is null then (value(b.call_days,0)+value(c.call_days,0))*1.0/2
when b.user_id is not null and c.user_id is null and d.user_id is null then value(b.call_days,0) else 0 end

,case when b.user_id is not null and c.user_id is not null and d.user_id is not null 
then (value(b.gprs_days,0)+value(c.gprs_days,0)+value(d.gprs_days,0))*1.0/3 
when b.user_id is not null and c.user_id is not null and d.user_id is null then (value(b.gprs_days,0)+value(c.gprs_days,0))*1.0/2
when b.user_id is not null and c.user_id is null and d.user_id is null then value(b.gprs_days,0) else 0 end
from report2.report_5G_term_model_test_mid9 a 
left join dw2.stat_commu_days_dt_1_prt_p20201231 b on a.user_id=b.user_id 
left join dw2.stat_commu_days_dt_1_prt_p20201130 c on a.user_id=c.user_id
left join dw2.stat_commu_days_dt_1_prt_p20201031 d on a.user_id=d.user_id;





drop table report2.report_5G_term_model_test_mid10_1;
create table report2.report_5G_term_model_test_mid10_1(
    user_id   varchar(20),       --用户编号
	phone_No  varchar(20),     --号码
	city_id   integer,        --地市
	county_id  varchar(20),     --办理区县
	create_date date,           --办理时间
	userstatus_id  integer,     --用户状态
	plan_id  varchar(20),     --服务计划
	cust_id   varchar(50),    --客户编号
	age    integer,        --年龄
	sex_id  integer,        --性别
	user_online  integer,     --网龄
	cust_type    varchar(10),   --客户类型1：个人，2：集团，3校园
	city_type   varchar(10),  ----城县农标志，1：城市，2：县城，3：农村，9：其他
	iden_no  varchar(50),      --证件号
	user_class   varchar(10),
	if_4G_cust   varchar(10),
    iden_num  integer,        --该证件下号码数
	if_qqt   integer,  --是否全球通
	if_jttf   integer,--是否集团统付
	imei     varchar(20),     --串号
	ji_ling  integer,        --机龄
	cond_id  varchar(20),     --押金
	busi_date  date,            --终端合约办理时间
	if_huanj   varchar(10),  --当月是否换机
	expire_date  date,       --到期时间
	termbrand_name  varchar(50),    --当前终端品牌
	price   integer,          --当前终端价格 
	price_fd  varchar(20),    --当前终端价格分档
	low_cost   decimal(20,2),  --合约最低消费
	if_hyj  integer,        --是否是合约机
	if_skc  varchar(20),    --是否双卡槽
	if_4G_term   integer,    --是否是4G终端
	expire_month  varchar(10),   --当前终端合约剩余期限
	huanj_month    varchar(10),  --用户换机时长
	--avg_hj       varchar(10),   --历史换机时长
	--latest_3_huanj   varchar(10), --近3年换机时长
	--termbrand_name_last  varchar(50),  --上次终端品牌
	huanj_month_last    varchar(10),  --上次终端使用时长
	family_5G     integer ,    --家庭网中是否5G
	group_5G      integer,    --集团网中是否5G
	
	plan_zifei   integer,       --套餐费
	flow_unit  decimal(20,2),   --套内流量
	if_dll  integer,            --是否大流量
	
   arpu1       decimal(20,2),    --T-1ARPU
   arpu2       decimal(20,2),    --T-2ARPU
   arpu3       decimal(20,2),    --T-3ARPU
   flow1       decimal(20,2),    --T-1DOU
   flow2       decimal(20,2),    --T-2DOU
   flow3       decimal(20,2),    --T-3DOU
   flow_bhd1  decimal(20,2),     --T-1饱和度
   flow_bhd2  decimal(20,2),     --T-2饱和度
   flow_bhd3  decimal(20,2),     --T-3饱和度
   avg_ctc_flow_fee   decimal(20,2),   --近3个月平均饱和度
   --call_days_avg      varchar(10), --近3月通话天数
   --gprs_days_avg      varchar(10), --近3月流量天数

   flow_23_zb    decimal(20,2),       --23G流量占比
   flow_23_zb_3  decimal(20,2),       --近3个月23G流量占比
   
   flow_yx    decimal(20,2),          --游戏流量
   flow_yy    decimal(20,2),          --语音流量
   flow_sp    decimal(20,2),         --视频流量
   flow_yx_zb   decimal(20,2),        --游戏流量占比
   flow_yy_zb   decimal(20,2),        --语音流量占比
   flow_sp_zb   decimal(20,2)         --视频流量占比
 --  if_5G_plan   integer        --是否5G套餐
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid10_1
select
a.user_id--用户编号
,a.phone_No--号码
,a.city_id --地市
,a.county_id--办理区县
,a.create_date--办理时间
,a.userstatus_id--用户状态
,a.plan_id--服务计划
,a.cust_id --客户编号
,a.age  --年龄
,a.sex_id--性别
,a.user_online--网龄
,a.cust_type --客户类型1：个人，2：集团，3校园
,city_type   ----城县农标志，1：城市，2：县城，3：农村，9：其他
,a.iden_no  --证件号
,a.user_class
,a.if_4G_cust
,a. iden_num--该证件下号码数
,a.if_qqt --是否全球通
,a.if_jttf--是否集团统付
,a.imei--串号
,a.ji_ling--机龄
,a.cond_id--押金
,a.busi_date  --终端合约办理时间
,a.if_huanj
,a.expire_date--到期时间
,a.termbrand_name--当前终端品牌
,a.price--当前终端价格 
,a.price_fd  --当前终端价格分档
,a.low_cost  --合约最低消费
,a.if_hyj--是否是合约机
,a.if_skc  --是否双卡槽
,a.if_4G_term--是否是4G终端
,a.expire_month  --当前终端合约剩余期限
,d.huanj_month    --换机时长
--,e.avg_hj        --历史终端换机时长
--,value(e.num,0)  --近3年换机次数
--,f.termbrand_name   --上次终端品牌
,d.huanj_month     --上次终端使用时长
,b.if_5G   --家庭网中是否有5G
,c.if_5G    --集团网中是否有5G
,a.plan_zifei--套餐费
,a.flow_unit  --套内流量
,a.if_dll --是否大流量
,a.arpu1  --T-1ARPU
,a.arpu2  --T-2ARPU
,a.arpu3  --T-3ARPU
,a.flow1  --T-1DOU
,a.flow2  --T-2DOU
,a.flow3  --T-3DOU
,a.flow_bhd1 --T-1饱和度
,a.flow_bhd2 --T-2饱和度
,a.flow_bhd3 --T-3饱和度
,a.avg_ctc_flow_fee--近3个月月均流量超套费
--,g.call_days_avg       --近3月通话天数
--,g.gprs_days_avg       --近3月流量天数
,a.flow_23_zb  --23G流量占比
,a.flow_23_zb_3--近3个月23G流量占比  
,a.flow_yx  --游戏流量
,a.flow_yy  --语音流量
,a.flow_sp --视频流量
,a.flow_yx_zb  --游戏流量占比
,a.flow_yy_zb  --语音流量占比
,a.flow_sp_zb--视频流量占比
--,case when h.offer_id is not null then 1 else 0 end  --是否是5G套餐
from report2.report_5G_term_model_test_mid9 a 
left join report2.report_family_5Gzd_mid12 b on a.user_id=b.user_id
left join report2.report_jituan_5G_mid12 c on a.phone_no=c.bill_id
left join report2.report_user_huanji_total_mid1 d on a.user_id=d.user_id;






drop table report2.report_5G_term_model_test_mid11;
create table report2.report_5G_term_model_test_mid11(
    user_id   varchar(20),       --用户编号
	phone_No  varchar(20),     --号码
	city_id   integer,        --地市
	county_id  varchar(20),     --办理区县
	create_date date,           --办理时间
	userstatus_id  integer,     --用户状态
	plan_id  varchar(20),     --服务计划
	cust_id   varchar(50),    --客户编号
	age    integer,        --年龄
	sex_id  integer,        --性别
	user_online  integer,     --网龄
	cust_type    varchar(10),   --客户类型1：个人，2：集团，3校园
	city_type   varchar(10),  ----城县农标志，1：城市，2：县城，3：农村，9：其他
	iden_no  varchar(50),      --证件号
	user_class   varchar(10),   --用户星级
	if_4G_cust   varchar(10),  --是否4G客户，集团口径
    iden_num  integer,        --该证件下号码数
	if_qqt   integer,  --是否全球通
	if_jttf   integer,--是否集团统付
	imei     varchar(20),     --串号
	ji_ling  integer,        --机龄
	cond_id  varchar(20),     --押金
	busi_date  date,            --终端合约办理时间
	if_huanj   varchar(10),  --当月是否换机
	expire_date  date,       --到期时间
	termbrand_name  varchar(50),    --当前终端品牌
	price   integer,          --当前终端价格 
	price_fd  varchar(20),    --当前终端价格分档
	low_cost   decimal(20,2),  --合约最低消费
	if_hyj  integer,        --是否是合约机
	if_skc  varchar(20),    --是否双卡槽
	if_4G_term   integer,    --是否是4G终端
	expire_month  varchar(10),   --当前终端合约剩余期限
	huanj_month    varchar(10),  --用户换机时长
	avg_hj       varchar(10),   --历史换机时长
	latest_3_huanj   varchar(10), --近3年换机次数
	termbrand_name_last  varchar(50),  --上次终端品牌
	huanj_month_last    varchar(10),  --上次终端使用时长
	family_5G     integer ,    --家庭网中是否5G
	group_5G      integer,    --集团网中是否5G
	
	plan_zifei   integer,       --套餐费
	flow_unit  decimal(20,2),   --套内流量
	if_dll  integer,            --是否大流量
	
   arpu1       decimal(20,2),    --T-1ARPU
   arpu2       decimal(20,2),    --T-2ARPU
   arpu3       decimal(20,2),    --T-3ARPU
   flow1       decimal(20,2),    --T-1DOU
   flow2       decimal(20,2),    --T-2DOU
   flow3       decimal(20,2),    --T-3DOU
   flow_bhd1  decimal(20,2),     --T-1饱和度
   flow_bhd2  decimal(20,2),     --T-2饱和度
   flow_bhd3  decimal(20,2),     --T-3饱和度
   avg_ctc_flow_fee   decimal(20,2),   --近3个月月均流量超套费
   call_days_avg      varchar(10), --近3月通话天数
   gprs_days_avg      varchar(10), --近3月流量天数

   flow_23_zb    decimal(20,2),       --23G流量占比
   flow_23_zb_3  decimal(20,2),       --近3个月23G流量占比
   
   flow_yx    decimal(20,2),          --游戏流量
   flow_yy    decimal(20,2),          --语音流量
   flow_sp    decimal(20,2),         --视频流量
   flow_yx_zb   decimal(20,2),        --游戏流量占比
   flow_yy_zb   decimal(20,2),        --语音流量占比
   flow_sp_zb   decimal(20,2),         --视频流量占比
   if_5G_plan   integer        --是否5G套餐
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(phone_No)                                                  
;
insert into report2.report_5G_term_model_test_mid11
select
a.user_id--用户编号
,a.phone_No--号码
,a.city_id--地市
,a.county_id--办理区县
,a.create_date--办理时间
,a.userstatus_id--用户状态
,a.plan_id--服务计划
,a.cust_id--客户编号
,a.age--年龄
,a.sex_id--性别
,a.user_online--网龄
,a.cust_type--客户类型1：个人，2：集团，3校园
,a.city_type----城县农标志，1：城市，2：县城，3：农村，9：其他
,a.iden_no--证件号
,a.user_class
,a.if_4G_cust
,a.iden_num--该证件下号码数
,a.if_qqt--是否全球通
,a.if_jttf--是否集团统付
,a.imei--串号
,a.ji_ling--机龄
,a.cond_id--押金
,a.busi_date--终端合约办理时间
,a.if_huanj
,a.expire_date--到期时间
,a.termbrand_name--当前终端品牌
,a.price--当前终端价格
,a.price_fd--当前终端价格分档
,a.low_cost--合约最低消费
,a.if_hyj--是否是合约机
,a.if_skc--是否双卡槽
,a.if_4G_term--是否是4G终端
,a.expire_month--当前终端合约剩余期限
,a.huanj_month--用户换机时长
,e.avg_hj        --历史终端换机时长
,value(e.num,0)  --近3年换机次数
,f.termbrand_name   --上次终端品牌
,a.huanj_month_last--上次终端使用时长
,a.family_5G--家庭网中是否5G
,a.group_5G--集团网中是否5G
,a.plan_zifei--套餐费
,a.flow_unit--套内流量
,a.if_dll--是否大流量
,a.arpu1--T-1ARPU
,a.arpu2--T-2ARPU
,a.arpu3--T-3ARPU
,a.flow1--T-1DOU
,a.flow2--T-2DOU
,a.flow3--T-3DOU
,a.flow_bhd1--T-1饱和度
,a.flow_bhd2--T-2饱和度
,a.flow_bhd3--T-3饱和度
,a.avg_ctc_flow_fee--近3个月月均流量超套费
,g.call_days_avg       --近3月通话天数
,g.gprs_days_avg       --近3月流量天数
,a.flow_23_zb--23G流量占比
,a.flow_23_zb_3--近3个月23G流量占比
,a.flow_yx--游戏流量
,a.flow_yy--语音流量
,a.flow_sp--视频流量
,a.flow_yx_zb--游戏流量占比
,a.flow_yy_zb--语音流量占比
,a.flow_sp_zb--视频流量占比
,case when h.offer_id is not null then 1 else 0 end  --是否是5G套餐
from report2.report_5G_term_model_test_mid10_1 a 
left join report2.report_latest_3_huanji_mid e on a.user_id=e.user_id
left join report2.report_scond_time_term_mid f on a.user_id=f.user_id
left join report2.report_call_gprs_days_mid g on a.user_id=g.user_id
left join dim2.report_5G_offer_type h on a.plan_id=h.offer_id;




--目标表【5G终端推荐模型需求-gp】
drop table report2.report_5G_terminal_recommend_model_ms;
create table report2.report_5G_terminal_recommend_model_ms(
	gp_date varchar(20),
     user_id   varchar(20),
	 phone_no   varchar(20),
	 age       varchar(10),
	 userstatus_id  varchar(10),
	 sex_id     varchar(10),
	 user_online  varchar(10),
	 cust_type    varchar(10),
	 city_type    varchar(10),
	 if_4G_cust   varchar(10),
	 user_class   varchar(10),
	 iden_num     varchar(10),
	 if_qqt       varchar(10),
	 if_jttf      varchar(10),
	 term_5G_name  varchar(50),
	 price_5G      varchar(10),
	 price_5g_fd   varchar(10),
	 if_5G_skc     varchar(10),
	 termbrand_name  varchar(50),
	 price    varchar(10),
	 price_fd  varchar(20),
	 if_skc   varchar(10),
	 ji_ling   varchar(10),
	 huanj_month     varchar(10),
	 if_4G_term      varchar(10),
	 if_hyj          varchar(10),
	 expire_month    varchar(10),
	 low_cost        decimal(20,2),
	 avg_hj          varchar(10),
	 latest_3_huanj   varchar(10),
	 termbrand_name_last   varchar(50),
	 huanj_month_last      varchar(10),
	 family_5G             varchar(10),
	 group_5G              varchar(10),
	 plan_zifei            varchar(10),
	 flow_unit             decimal(20,2),
	 if_dll                varchar(10),
	 arpu1              decimal(20,2),
	 arpu2              decimal(20,2),
	 arpu3              decimal(20,2),
	 flow1              decimal(20,2),
	 flow2              decimal(20,2),
	 flow3              decimal(20,2),
	 flow_bhd1          decimal(20,2),
	 flow_bhd2          decimal(20,2),
	 flow_bhd3          decimal(20,2),
	 avg_ctc_flow_fee   decimal(20,2),
	 call_days_avg      decimal(20,2),
	 gprs_days_avg      decimal(20,2),
	 last_xianshu_num   varchar(10),
	 flow_23_zb         decimal(20,2),
	 latest_3_xianshu_num    varchar(10),
	 flow_23_zb_3            decimal(20,2),
	 flow_yx             decimal(20,2),
	 flow_yy             decimal(20,2),
	 flow_sp             decimal(20,2),
	 flow_yx_zb          decimal(20,2),
     flow_yy_zb          decimal(20,2),
     flow_sp_zb          decimal(20,2),
     if_5G_plan          varchar(10),
	 jizhan_3            varchar(10),
	 if_huanj            varchar(10)
)WITH (  OIDS=FALSE,appendonly=true, compresslevel=6, compresstype=zlib) 
tablespace tbs_report2                                                      
DISTRIBUTED BY(user_id) 
partition by list(gp_date) (partition p202012 values('202012')); 

insert into report2.report_5G_terminal_recommend_model_ms
select
'202012'
,a.user_id
,a.phone_no
,a.age
,a.userstatus_id
,a.sex_id
,a.user_online
,a.cust_type
,a.city_type
,a.if_4G_cust
,a.user_class
,a.iden_num
,a.if_qqt
,a.if_jttf
,''
,''
,''
,''
,a.termbrand_name
,a.price
,a.price_fd
,a.if_skc
,a.ji_ling    --当前终端使用时长
,a.huanj_month
,a.if_4G_term
,a.if_hyj
,a.expire_month
,a.low_cost
,a.avg_hj
,a.latest_3_huanj
,a.termbrand_name_last
,a.huanj_month_last
,a.family_5G
,a.group_5G
,a.plan_zifei
,a.flow_unit
,a.if_dll
,a.arpu1
,a.arpu2
,a.arpu3
,a.flow1
,a.flow2
,a.flow3
,a.flow_bhd1 
,a.flow_bhd2
,a.flow_bhd3 
,a.avg_ctc_flow_fee 
,a.call_days_avg::decimal(20,2) 
,a.gprs_days_avg::decimal(20,2)
,''
,flow_23_zb
,''
,a.flow_23_zb_3
,a.flow_yx   
,a.flow_yy    
,a.flow_sp    
,a.flow_yx_zb  
,a.flow_yy_zb  
,a.flow_sp_zb  
,a.if_5G_plan  
,''
,a.if_huanj
from report2.report_5G_term_model_test_mid11 a ;




