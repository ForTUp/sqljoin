-- Function: report2.p_report_5g_terminal_recommend_model_all_ms(character varying)
-- DROP FUNCTION report2.p_report_5g_terminal_recommend_model_all_ms(character varying);

CREATE OR REPLACE FUNCTION report2.p_report_5g_terminal_recommend_model_all_ms(IN i_mon character varying,OUT o_return_code character varying,OUT o_return_msg character varying)
  RETURNS record AS
$BODY$
DECLARE
--***********************************************************************************************
-- sql 存储过程
-- 名称     : report2.p_report_5g_terminal_recommend_model_all_ms
-- 注意事项 ：
-- 参数     ：i_mon:       统计日期
--            o_return_code：返回值
--            o_return_msg： 返回信息
-- 功能描述 ：5终端推荐模型合并-gp
-- 返回值   ：0 正确; -1 错误
-- 是否校验： 否
-- 输入表   :  
-- 输出表   :  report2.report_5g_terminal_recommend_model_all_ms
-- 创建日期 ： 2021-01-26 09:12:27
-- 业务负责人: 漆海游
-- 技术负责人：漆海游
-- 需求编码:
-- 运行时长：
-- 创建人   ：xnp
-- 修改历史 ：修改人     修改时间    主要改动说明
--***********************************************************************************************
    vd_first_date        date;          --该帐期第一天的日期
    vd_end_date          date;          --该帐期最后一天的日期
    vi_acyc_id           integer;       --存放帐期月acyc_id
    vs_bcyc_id           varchar(20);   --存放账期月份
    vs_dynstr1           varchar;  --存放动态sql执行语句
    vs_proc_name         varchar(50);   --存放日志表存储过程名
    vs_proc_para         varchar;       --存放日志表存储过程参数
    vi_step_no           integer;       --存放日志表步骤序号
    vs_step_desc         varchar(100);  --存放日志表步骤描述
    vi_row_count         integer;      --影响记录数
    vi_result            integer;      --用于调用其它过程存储返回值
    vs_table_schema      varchar;      --目标表的模式名
    vs_table_name        varchar;      --目标表表明
    vi_mon_days          integer;      --当月天数
    vi_pre_mon           integer;      --上月
    vi_pre_year          integer;      --去年
    vs_year              varchar(4);   --当年
    vs_enddate           varchar(8);          --该帐期最后一天的日期
BEGIN
    o_return_code := 0;
    o_return_msg := '存储过程未运行!';
    vs_proc_name := 'report2.p_report_5g_terminal_recommend_model_all_ms';
    vs_bcyc_id   := i_mon;
    vs_proc_para := i_mon;
    vs_table_schema := 'report2';
    vs_table_name   := 'report_5g_terminal_recommend_model_all_ms';
    vi_pre_mon :=cast(dim2.p_func_deal_date(i_mon,2,-1) as int);--上月 yyyymm
    vi_pre_year :=cast(dim2.p_func_deal_date(i_mon,2,-12) as int);--去年 yyyymm
    vd_first_date :=date(text(int4(i_mon)*100 +1));--当月第一天 yyyy-mm-dd
    vd_end_date :=date(text(dim2.p_func_deal_date(text(int4(dim2.p_func_deal_date(i_mon,2,1))*100 +1),1,-1)));--当月最后一天 yyyy-mm-dd
    vs_enddate :=dim2.p_func_deal_date(text(int4(dim2.p_func_deal_date(i_mon,2,1))*100 +1),1,-1);--当月最后一天 yyyymmdd
    vi_mon_days :=dim2.p_func_get_mon_days(cast(i_mon as int)); --当月天数
    vs_year :=substr(vs_bcyc_id,0,5);
    ---------------------------------------------------------------------------------------------
    --向日志表插入数据
    ---------------------------------------------------------------------------------------------
    vi_step_no := 0;
    vs_step_desc := '存储过程运行开始!';
    vi_row_count := 0;
    raise info '[ % ]【STEP % %】', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);
    ---------------------------------------------------------------------------------------------  
    --增加当周期分区
    -----------------------------------------------------------------------------------------------
    vi_step_no   := 1;
    vs_step_desc := '增加当周期分区';
    vi_row_count :=0;
    --SQL
    vs_dynstr1 := 'select 1 from dim2.p_func_add_partition('''||vs_bcyc_id||''','''||vs_table_schema||''','''||vs_table_name||''')';--增加分区过程(日期,模式名,表名)
    --SQL
    raise info '[ % ]【STEP % % START】: %;', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vs_dynstr1;
    execute vs_dynstr1 into vi_result;
    GET DIAGNOSTICS  vi_row_count = ROW_COUNT; 
    raise info '[ % ]【STEP % % COMMIT】: % ', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vi_row_count;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);
    ---------------------------------------------------------------------------------------------  
    --清空表当前周期数据
    ----------------------------------------------------------------------------------------------
    vi_step_no := 2;
    vs_step_desc := '清空表当前周期数据';
    vi_row_count :=0;
    --SQL
    vs_dynstr1 := 'truncate '||vs_table_schema||'.'||vs_table_name||'_1_prt_p'||vs_bcyc_id; 
    --SQL
    raise info '[ % ]【STEP % % START】: %;', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vs_dynstr1;
    execute vs_dynstr1;
    GET DIAGNOSTICS  vi_row_count = ROW_COUNT; 
    raise info '[ % ]【STEP % % COMMIT】: % ', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vi_row_count;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);
    ---------------------------------------------------------------------------------------------
    --生成report2.report_5g_terminal_recommend_model_all_ms
    ---------------------------------------------------------------------------------------------
    vi_step_no := 3;
    vs_step_desc := '生成report2.report_5g_terminal_recommend_model_all_ms';
    vi_row_count :=0;
    --SQL
    vs_dynstr1 := '		insert into report2.report_5g_terminal_recommend_model_all_ms
		select 
		 ''202012''
		,a.user_id
		,a.phone_no
		,a.age
		,a.userstatus_id
		,case when a.sex_id::int=1 then 0 
		when a.sex_id::int=2 then 1 
		else 2 end  --0:男，1女，2：未知
		,a.user_online
		,a.cust_type
		,a.city_type
		,a.if_4g_cust
		,a.user_class
		,a.iden_num
		,a.if_qqt
		,a.if_jttf
		,a.term_5g_name
		,a.price_5g
		,a.price_5g_fd
		,a.if_5g_skc
		,a.termbrand_name
		,a.price
		,case when value(a.price::int,0)>0 and value(a.price::int,0)<=800 then ''(0-800]''
		      when value(a.price::int,0)>800 and value(a.price::int,0)<=1800 then ''(800-1800]''
			  when value(a.price::int,0)>1800 and value(a.price::int,0)<=3000 then ''(1800-3000]''
			  when value(a.price::int,0)>3000 and value(a.price::int,0)<=4000 then ''(3000-4000]''
			   when value(a.price::int,0)>4000 and value(a.price::int,0)<=5000 then ''(4000-5000]''
			   when value(a.price::int,0)>5000  then ''(5000-++]'' end
		,a.if_skc
		,a.ji_ling
		,case when a.huanj_month::int=999 then ''null'' else a.huanj_month end
		,a.if_4g_term
		,a.if_hyj
		,a.expire_month
		,a.low_cost
		,value(c.num1,0)
		,value(c.num,0)
		,a.termbrand_name_last
		,case when a.huanj_month_last::int=999 then ''null'' else a.huanj_month_last end
		,a.family_5g
		,a.group_5g
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
		,a.call_days_avg
		,a.gprs_days_avg
		,a.last_xianshu_num
		,a.flow_23_zb
		,a.latest_3_xianshu_num
		,a.flow_23_zb_3
		,a.flow_yx         
		,a.flow_yy         
		,a.flow_sp         
		,a.flow_yx_zb      
		,a.flow_yy_zb      
		,a.flow_sp_zb
		,a.if_5g_plan
		,a.jizhan_3
		,1
		,'''' 
		from report2.report_5g_terminal_recommend_model_ms_1_prt_p202012 a
		left join (select user_id,count(1) num,36/count(1) num1
		from dw2.dw_td_intelligence_machine_ywj_mid2_1_prt_p20201231  
		where op_time>=''2017-03-01'' and op_time<=''2020-12-31''
		group by user_id) c on a.user_id=c.user_id
		left join (select user_id from  report2.report_5g_terminal_recommend12_model_mm )  d  on  a.user_id=d.user_id
		where  d.user_id  is null 
		;

    ';
    --SQL
    raise info '[ % ]【STEP % % START】: %;', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vs_dynstr1;
    execute vs_dynstr1;
    GET DIAGNOSTICS  vi_row_count = ROW_COUNT;
    raise info '[ % ]【STEP % % COMMIT】: % ', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vi_row_count;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);
    ---------------------------------------------------------------------------------------------  
    --增加当周期分区
    -----------------------------------------------------------------------------------------------
    vi_step_no   := 4;
    vs_step_desc := '增加当周期分区';
    vi_row_count :=0;
    --SQL
    vs_dynstr1 := 'select 1 from dim2.p_func_add_partition('''||vs_bcyc_id||''','''||vs_table_schema||''','''||vs_table_name||''')';--增加分区过程(日期,模式名,表名)
    --SQL
    raise info '[ % ]【STEP % % START】: %;', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vs_dynstr1;
    execute vs_dynstr1 into vi_result;
    GET DIAGNOSTICS  vi_row_count = ROW_COUNT; 
    raise info '[ % ]【STEP % % COMMIT】: % ', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vi_row_count;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);
    ---------------------------------------------------------------------------------------------  
    --清空表当前周期数据
    ----------------------------------------------------------------------------------------------
    vi_step_no := 5;
    vs_step_desc := '清空表当前周期数据';
    vi_row_count :=0;
    --SQL
    vs_dynstr1 := 'truncate '||vs_table_schema||'.'||vs_table_name||'_1_prt_p'||vs_bcyc_id; 
    --SQL
    raise info '[ % ]【STEP % % START】: %;', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vs_dynstr1;
    execute vs_dynstr1;
    GET DIAGNOSTICS  vi_row_count = ROW_COUNT; 
    raise info '[ % ]【STEP % % COMMIT】: % ', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vi_row_count;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);
    ---------------------------------------------------------------------------------------------
    --生成report2.report_5g_terminal_recommend_model_all_ms
    ---------------------------------------------------------------------------------------------
    vi_step_no := 6;
    vs_step_desc := '生成report2.report_5g_terminal_recommend_model_all_ms';
    vi_row_count :=0;
    --SQL
    vs_dynstr1 := '		insert into report2.report_5g_terminal_recommend_model_all_ms
		select 
		''202012''
		,a.user_id
		,a.phone_no
		,a.age
		,a.userstatus_id
		,a.sex_id
		,a.user_online
		,a.cust_type
		,a.city_type
		,a.if_4g_cust
		,a.user_class
		,a.iden_num
		,a.if_qqt
		,a.if_jttf
		,a.term_5g_name
		,a.price_5g
		,case when value(a.price_5g::int,0)>0 and value(a.price_5g::int,0)<=800 then ''(0-800]''
		      when value(a.price_5g::int,0)>800 and value(a.price_5g::int,0)<=1800 then ''(800-1800]''
			  when value(a.price_5g::int,0)>1800 and value(a.price_5g::int,0)<=3000 then ''(1800-3000]''
			  when value(a.price_5g::int,0)>3000 and value(a.price_5g::int,0)<=4000 then ''(3000-4000]''
			   when value(a.price_5g::int,0)>4000 and value(a.price_5g::int,0)<=5000 then ''(4000-5000]''
			   when value(a.price_5g::int,0)>5000  then ''(5000-++]'' end
		,a.if_5g_skc
		,a.termbrand_name
		,a.price
		,case when value(a.price::int,0)>0 and value(a.price::int,0)<=800 then ''(0-800]''
		      when value(a.price::int,0)>800 and value(a.price::int,0)<=1800 then ''(800-1800]''
			  when value(a.price::int,0)>1800 and value(a.price::int,0)<=3000 then ''(1800-3000]''
			  when value(a.price::int,0)>3000 and value(a.price::int,0)<=4000 then ''(3000-4000]''
			   when value(a.price::int,0)>4000 and value(a.price::int,0)<=5000 then ''(4000-5000]''
			   when value(a.price::int,0)>5000  then ''(5000-++]'' end
		,a.if_skc
		,a.ji_ling
		,case when a.huanj_month::int=999 then ''null'' else a.huanj_month end
		,a.if_4g_term
		,a.if_hyj
		,a.expire_month
		,a.low_cost
		,value(c.num1,0)
		,value(c.num,0)
		,a.termbrand_name_last ---,d.termbrand_name
		,case when a.huanj_month_last::int=999 then ''null'' else a.huanj_month_last end
		,a.family_5g
		,a.group_5g
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
		,a.call_days_avg
		,a.gprs_days_avg
		,a.last_xianshu_num
		,a.flow_23_zb
		,a.latest_3_xianshu_num
		,a.flow_23_zb_3
		
		,a.flow_yx         
		,a.flow_yy         
		,a.flow_sp         
		,a.flow_yx_zb      
		,a.flow_yy_zb      
		,a.flow_sp_zb      
		
		,a.if_5g_plan
		,a.jizhan_3
		,2
		,b.op_time
		from report2.report_5g_terminal_recommend12_model_mm a
		left join report2.report_5g_terminal_now_recommend_mid b on a.user_id=b.user_id
		left join (select user_id,count(1) num,36/count(1) num1
		from dw2.dw_td_intelligence_machine_ywj_mid2_1_prt_p20201231
		where op_time>=''2017-03-01'' and op_time<=''2020-12-31''
		group by user_id) c on a.user_id=c.user_id
		left join report2.report_scond_time1_term_mid d on a.user_id=d.user_id
		;

    ';
    --SQL
    raise info '[ % ]【STEP % % START】: %;', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vs_dynstr1;
    execute vs_dynstr1;
    GET DIAGNOSTICS  vi_row_count = ROW_COUNT;
    raise info '[ % ]【STEP % % COMMIT】: % ', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vi_row_count;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);
    ---------------------------------------------------------------------------------------------
    --更新最大日期
    ---------------------------------------------------------------------------------------------
    vi_step_no := 997;
    vs_step_desc := '最大日期';
    vi_row_count :=0;
    --SQL
    vs_dynstr1 := '
    update report2.REPORT_MAXDATE_INFO set MAX_DATE = '''||vd_first_date||'''  where TABLE_NAME=''report2.report_5g_terminal_recommend_model_all_ms''
		';
    --SQL
    raise info '[ % ]【STEP % % START】: %;', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vs_dynstr1;
    execute vs_dynstr1;
    GET DIAGNOSTICS  vi_row_count = ROW_COUNT;
    raise info '[ % ]【STEP % % COMMIT】: % ', substr(text(clock_timestamp()),0,23),vi_step_no,vs_step_desc,vi_row_count;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);

    ---------------------------------------------------------------------------------------------
    --删除超过历史周期数据
    -----------------------------------------------------------------------------------------------
    vi_step_no   := 998;
    vs_step_desc := '删除超过历史周期数据';
    vi_row_count :=0;
    --[sql]
    vs_dynstr1 := 'select 1 from dim2.p_func_del_data_day('''||vs_bcyc_id||''','''||vs_table_schema||''','''||vs_table_name||''');';--删除超期数据(日期,模式名,表名)
    --[sql]
    raise info '[ % ]【% START】: %;', substr(text(clock_timestamp()),0,23),vs_step_desc,vs_dynstr1;
    execute vs_dynstr1 into vi_result;
    GET DIAGNOSTICS  vi_row_count = ROW_COUNT;
    raise info '[ % ]【% COMMIT】: % ', substr(text(clock_timestamp()),0,23),vs_step_desc,vi_row_count;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);

    -----------------------------------------------------------------------------------------------*
    --存储过程执行成功
    ---------------------------------------------------------------------------------------------
    vi_step_no := 999;
    vs_step_desc := '存储过程运行结束!';
    vi_row_count := 0;
    insert into dim2.dim_proc_run_log(proc_name,proc_para,step_no,step_desc,step_over_time,step_records) values (vs_proc_name,vs_proc_para,vi_step_no,vs_step_desc,clock_timestamp(),vi_row_count);
    o_return_code := 0 ;
    o_return_msg := 'FUNCTION '||vs_proc_name||' FINISH SUCCESSFULLY!';
    raise info '[ % ]【%】 ', substr(text(clock_timestamp()),0,23),o_return_msg;
    RETURN;

    EXCEPTION WHEN OTHERS THEN
        o_return_code := SQLSTATE;
        o_return_msg  := 'SQL'||text(vi_step_no)||':'||vs_step_desc||'出错! 错误信息:'||SQLERRM;
        raise warning '【SQLSTATE： %  MSG: % 】', o_return_code,o_return_msg;
    RETURN;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
