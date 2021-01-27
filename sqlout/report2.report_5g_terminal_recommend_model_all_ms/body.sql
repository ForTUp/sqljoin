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
