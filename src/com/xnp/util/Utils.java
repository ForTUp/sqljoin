package com.xnp.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;





public class Utils {

	/**
	  *  创建文件
	 * @param path
	 * @throws IOException
	 */
	public static void createFile(String path) throws IOException {
	    if (path!=null && path!="") {
	        File file = new File(path);
	        if (!file.getParentFile().exists()) {
	            file.getParentFile().mkdirs();
	        }
	        file.createNewFile();
	    }

	}
	
	/**
	 * 存储过程尾部输出
	 * @throws Exception
	 */
	public static String bodyPath(Params params,String fileName) throws Exception{
		InputStream is = SqlHead.class.getResourceAsStream("/com/xnp/model/comm/"+fileName);
		BufferedReader br  = new BufferedReader(new InputStreamReader(is, "utf-8"));
		//读写文件行
		String tmp;
		//结果输出字符串
		String result = "";
		while((tmp=br.readLine())!=null){
			tmp = Utils.replaceVaria(tmp,params);
			result  = result + tmp + "\n";
		}
		br.close();
		is.close();
		//计步器加1
		params.setStep_no(params.getStep_no()+1);
		return result;
	}
	
	/**
	 * 合并文件
	 * @throws Exception
	 */
	public static void fileMerge(List<String > list,String outPath) throws Exception{
		//合并文件
		File tmpFile = new File(outPath);
		Utils.createFile(outPath);
		@SuppressWarnings("resource")
//		FileWriter writer= new FileWriter(tmpFile, false);
		BufferedWriter writer = new BufferedWriter (new OutputStreamWriter(new FileOutputStream (tmpFile,false),"UTF-8"));
		for (int i = 0; i < list.size(); i++) {
			File file = new File(list.get(i));
			if (!file.exists() || file.isDirectory()) {
				System.out.println("合并文件异常!");
				return;
			}
			InputStream is = new BufferedInputStream(new FileInputStream(file));
			BufferedReader br  = new BufferedReader(new InputStreamReader(is, "utf-8"));
			//读写文件行
			String tmp;
			while((tmp=br.readLine())!=null){
				writer.write(tmp+"\n");
			}
			
			br.close();
			is.close();
		}
		writer.flush();
		writer.close();
	}
	
	/**
	 *  参数替换
	 * @param param
	 * @param params
	 * @return
	 * @throws Exception
	 */
	public static String replaceVaria(String param,Params params) throws Exception{
		param = param.replace(VariableName.func_full_name.getVariableName(), params.getFunc_full_name());
		param = param.replace(VariableName.func_name.getVariableName(), params.getFunc_name());
		param = param.replace(VariableName.out_table.getVariableName(), params.getOut_table());
		param = param.replace(VariableName.create_date.getVariableName(), params.getCreate_date());
		param = param.replace(VariableName.create_name.getVariableName(), params.getCreate_name());
		param = param.replace(VariableName.schema.getVariableName(), params.getSchema());
		param = param.replace(VariableName.table.getVariableName(), params.getTable());
		if (params.getCurrent_table()!=null && params.getCurrent_table()!="") {
			param = param.replace(VariableName.curren_table.getVariableName(), params.getCurrent_table());
		}
		if (params.getInsert_part()!=null && params.getInsert_part()!="") {
			param = param.replace(VariableName.insert_part.getVariableName(), params.getInsert_part().replace("'", "''"));
		}
		param = param.replace(VariableName.step_no.getVariableName(), String.valueOf(params.getStep_no()));
		return param;
	}
	
	/**
	 * 获取字符串编码
	 * @param str
	 * @return
	 */
	public static String getEncoding(String str) {
		String encode = "GB2312";
		try {
			if (str.equals(new String(str.getBytes(encode), encode))) { //判断是不是GB2312
				String s = encode;
				return s; //是的话，返回“GB2312“，以下代码同理
			}
		} catch (Exception exception) {
		}
		encode = "ISO-8859-1";
		try {
			if (str.equals(new String(str.getBytes(encode), encode))) { //判断是不是ISO-8859-1
				String s1 = encode;
				return s1;
			}
		} catch (Exception exception1) {
		}
		encode = "UTF-8";
		try {
			if (str.equals(new String(str.getBytes(encode), encode))) { //判断是不是UTF-8
				String s2 = encode;
				return s2;
			}
		} catch (Exception exception2) {
		}
		encode = "GBK";
		try {
			if (str.equals(new String(str.getBytes(encode), encode))) { //判断是不是GBK
				String s3 = encode;
				return s3;
			}
		} catch (Exception exception3) {
			
		}
		return "";
	}
	
	/**
	 * 转成utf8
	 * @param str
	 * @return
	 */
	public static String toChinese(String strvalue, String encodeFormat) {		
		String result = null;
		try {			
			if (strvalue == null) {				
				return "";			
			} else {				
				result = new String(strvalue.getBytes("GBK"), "UTF-8");					
				return result;			
			}		
		} catch (Exception e) {			
			return "";		
		}	
	}
	
}
