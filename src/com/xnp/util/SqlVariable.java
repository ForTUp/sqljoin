package com.xnp.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class SqlVariable {

	/**
	 * 存储过程参数输出
	 * @throws Exception
	 */
	public static String sqlVariable(Params params) throws Exception{
		String path = params.getOutPath()+File.separator+params.getOut_table()+File.separator+"variable.sql";
		File tmpFile = new File(path);
		Utils.createFile(path);
		InputStream is = null;
		if (params.getModelType()==ModelType.MONTH) {
			is = SqlHead.class.getResourceAsStream("/com/xnp/model/mon/mon_variable.txt");
		}else {
			is = SqlHead.class.getResourceAsStream("/com/xnp/model/day/day_variable.txt");
		}
		BufferedReader br  = new BufferedReader(new InputStreamReader(is, "utf-8"));
//		FileWriter writer= new FileWriter(tmpFile, false);
		BufferedWriter writer = new BufferedWriter (new OutputStreamWriter(new FileOutputStream (tmpFile,false),"UTF-8"));
		String tmp;
		while((tmp=br.readLine())!=null){
			tmp = Utils.replaceVaria(tmp,params);
			writer.write(tmp+"\n");
		}
		writer.flush();
		writer.close();
		return tmpFile.getPath();
	}
}
