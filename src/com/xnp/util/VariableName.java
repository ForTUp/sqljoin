package com.xnp.util;
/**
 * 
 * @author xnp
 *
 */
public enum VariableName {
	func_full_name ("{func_full_name}","存储过程名称"),
	func_name("{func_name}","功能描述"),
	out_table("{out_table}","输出表名全部"),
	table("{table}","输出表名不加schema"),
	create_date("{create_date}","创建日期"),
	create_name("{create_name}","创建人"),
	schema("{schema}","vs_table_schema"),
	step_no("{step_no}","中间表步数"),
	insert_part("{insert_part}","sql插入语句"),
	curren_table("{current_table}","当前表名")
	;
	
	private String variableName;
	private String chineseName ;
	
	private VariableName (String variableName,String chineseName) {
		this.variableName = variableName ;
        this.chineseName = chineseName ;
		
	}
	
	public String getVariableName() {
		return variableName;
	}
	public void setVariableName(String variableName) {
		this.variableName = variableName;
	}
	public String getChineseName() {
		return chineseName;
	}
	public void setChineseName(String chineseName) {
		this.chineseName = chineseName;
	}
	
    
    
     
    
}
