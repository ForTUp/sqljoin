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
