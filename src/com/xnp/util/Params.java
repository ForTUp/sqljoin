package com.xnp.util;

import java.io.Serializable;

/**
 * 
 * @author xnp
 *
 */
public class Params implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 存储过程名称
	 */
	private String func_full_name;
	/**
	 * 功能描述
	 */
	private String func_name;
	/**
	 * 输出表
	 */
	private String out_table;
	/**
	 * 输出表名不加schema
	 */
	private String table;
	/**
	 * 创建日期
	 */
	private String create_date;
	/**
	 * 创建人
	 */
	private String create_name;
	/**
	 * vs_table_schema
	 */
	private String schema;
	/**
	 * 中间表步数
	 */
	private int step_no;
	/**
	 * 模板类型 日 月
	 */
	private ModelType modelType;
	/**
	 * 文件输出路径
	 */
	private String outPath;
	/**
	 * 插入sql
	 */
	private String insert_part;
	/**
	 * 当前执行表名
	 */
	private String current_table;
	
	public String getFunc_full_name() {
		return func_full_name;
	}
	public void setFunc_full_name(String func_full_name) {
		this.func_full_name = func_full_name;
	}
	public String getFunc_name() {
		return func_name;
	}
	public void setFunc_name(String func_name) {
		this.func_name = func_name;
	}
	public String getOut_table() {
		return out_table;
	}
	public void setOut_table(String out_table) {
		this.out_table = out_table;
	}
	public String getCreate_date() {
		return create_date;
	}
	public void setCreate_date(String create_date) {
		this.create_date = create_date;
	}
	public String getCreate_name() {
		return create_name;
	}
	public void setCreate_name(String create_name) {
		this.create_name = create_name;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public int getStep_no() {
		return step_no;
	}
	public void setStep_no(int step_no) {
		this.step_no = step_no;
	}
	public ModelType getModelType() {
		return modelType;
	}
	public void setModelType(ModelType modelType) {
		this.modelType = modelType;
	}
	public String getOutPath() {
		return outPath;
	}
	public void setOutPath(String outPath) {
		this.outPath = outPath;
	}
	public String getCurrent_table() {
		return current_table;
	}
	public void setCurrent_table(String current_table) {
		this.current_table = current_table;
	}
	public String getInsert_part() {
		return insert_part;
	}
	public void setInsert_part(String insert_part) {
		this.insert_part = insert_part;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	
	
}
