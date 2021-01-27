package com.xnp.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author xnp
 *
 */
public class SqlBody {

	/**
	 * 中间sql拼接
	 * @param tableList 
	 * @param params 
	 * @return
	 * @throws Exception
	 */
	public static String sqlBody(Params params, Map<String, List<String>> tableMap) throws Exception {
		//sql集合
		List<String> tableSqlList = tableMap.get("tableSqlList");
		//表名集合
		List<String> tableNameList = tableMap.get("tableNameList");
		//计步器
		int num = 0;
		//中间主体sql文件
		String path = params.getOutPath()+File.separator+params.getOut_table()+File.separator+"body.sql";
		File tmpFile = new File(path);
		Utils.createFile(path);
		//计步器
		params.setStep_no(num);
//		FileWriter writer= new FileWriter(tmpFile, false);
		BufferedWriter writer = new BufferedWriter (new OutputStreamWriter(new FileOutputStream (tmpFile,false),"UTF-8"));
		for (int i = 0; i < tableSqlList.size(); i++) {
			String result="";
			//当前插入表
			String sql = tableSqlList.get(i);
			params.setCurrent_table(tableNameList.get(i));
			params.setInsert_part(sql);
			//第一步:开始时插入日志
			if (i==0) {
				result = Utils.bodyPath(params, "body1.txt");
				writer.write(result);
			}
			//如果是结果表，要先加分区
			if (tableNameList.get(i).contains(params.getOut_table()) && !tableNameList.get(i).contains(params.getOut_table()+"_")) {
				//要先加分区
				result = Utils.bodyPath(params, "body4.txt");
				writer.write(result);
				//清空当前分区数据
				result = Utils.bodyPath(params, "body5.txt");
				writer.write(result);
				//生成结果表
				result = Utils.bodyPath(params, "body6.txt");
				writer.write(result);
			}else {
				//不是结果表，第二步：清空数据
				result = Utils.bodyPath(params, "body2.txt");
				writer.write(result);
				//第三部：生成中间表数据
				result = Utils.bodyPath(params, "body3.txt");
				writer.write(result);
			}
		}
		writer.flush();
		writer.close();
		return tmpFile.getPath();
	}
}
