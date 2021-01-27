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
