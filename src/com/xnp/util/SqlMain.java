package com.xnp.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;



/**
 * 
 * @author xnp
 *
 */
public class SqlMain {

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		boolean quit = true;
		out(sc);
		while (quit) {
			int type = 0;
			try {
				type = sc.nextInt();
			} catch (Exception e) {
				System.err.println("请输入数字！");
				out(sc);
				sc.nextLine();
				continue;
			}
			if (type==1) {
				String result = sqlout(sc);
				System.out.println(result);
				out(sc);
			}else if (type==2) {
				System.out.println("已退出！");
				sc.close();
				return;
			}else {
				System.out.println("已退出！");
				sc.close();
				return;
			}
		}
	}
	
	@SuppressWarnings("resource")
	public static String sqlout(Scanner sc) {
		sc.nextLine();
		System.out.print("请您输入文件：");
		String inPath = sc.nextLine().trim();
		//输入文件名
		//输出文件路径
		//获取文件
		File file = new File(inPath);
		if (!file.exists()) {
			return "输入文件有误！";
		}
		//将文件放入缓存区
		InputStream is = null;
		try {
			is = new BufferedInputStream(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			return "文件找不到！";
		}
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(is, "utf-8"));
		} catch (UnsupportedEncodingException e) {
			return e.getMessage();
		}
		//替换参数
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		Params params = new Params();
		params.setFunc_name(file.getName().replace(".sql", ""));
		params.setCreate_date(dtf.format(LocalDateTime.now()));
		//创建人
		System.out.print("请您输入创建人：");
		String create_user = sc.nextLine().trim();
		params.setCreate_name(create_user);
		params.setStep_no(0);
		//存储过程模板
		System.out.print("请您输入存储过程模板（1，月模板；2，日模板）：");
		int mod = 0;
		try {
			mod = sc.nextInt();
		} catch (Exception e) {
			return "请输入数字！";
		}
		if (mod!=1 && mod!=2) {
			return "模板输入有误";
		}
		params.setModelType(mod==1?ModelType.MONTH:ModelType.DAY);
		sc.nextLine();
		//E:\sqlout
		System.out.println("请输入文件输出地址：");
		String outPath =sc.nextLine().trim();
		params.setOutPath(outPath);
		//逐行读取
		String tmp;
		//表明参数获取标记
		boolean flag = false;
		//分段插入表标记（以insert开始 ; 结尾）
		boolean tableFlag = false;
		//sql集合
		List<String> tableSqlList = new ArrayList<String>();
		//表明集合
		List<String> tableNameList = new ArrayList<String>();
		String tableName = "";
		String tableStr = "";
		Map<String, List<String>> tableMap = new HashMap<String, List<String>>();
		//注释标记
		boolean zsFlag = false;
		try {
			while((tmp=br.readLine())!=null){
				//全取小写
				tmp = tmp.toLowerCase();
				
				//获取表明及参数
				if (flag) {
					if (tmp.indexOf(";")>0) {
						params.setFunc_full_name(tmp.substring(tmp.indexOf("table")+6,tmp.indexOf(";")).replace(".", ".p_").trim());
						params.setOut_table(tmp.substring(tmp.indexOf("table")+6,tmp.indexOf(";")).trim());
					}else if (tmp.indexOf("(")>0) {
						params.setFunc_full_name(tmp.substring(tmp.indexOf("table")+6,tmp.indexOf("(")).replace(".", ".p_").trim());
						params.setOut_table(tmp.substring(tmp.indexOf("table")+6,tmp.indexOf("(")).trim());
					}else{
						params.setFunc_full_name(tmp.substring(tmp.indexOf("table")+6).replace(".", ".p_").trim());
						params.setOut_table(tmp.substring(tmp.indexOf("table")+6).trim());
					}
					params.setSchema(params.getFunc_full_name().substring(0,params.getFunc_full_name().indexOf(".")).trim());
					params.setTable(params.getOut_table().substring(params.getOut_table().indexOf(".")+1).trim());
					flag = false;
				}
				if (tmp.contains("目标表") ) {
					flag = true;
				}
				//先去除注释
				if (tmp.trim().startsWith("--")) {
					continue;
				}
				//注释开始
				if (tmp.trim().contains("/*")) {
					zsFlag = true;
				}
				//非注释代码，提取insert语句
				if (!zsFlag) {
					//将每个插入语句都放入list
					if (tmp.contains("insert")) {
						if (tmp.contains("select")) {
							tableName = tmp.substring(tmp.indexOf("into")+5,tmp.indexOf("select")).trim();
							tableFlag = true;
						}else {
							tableName = tmp.substring(tmp.indexOf("into")+5).trim();
							tableFlag = true;
						}
						
					}
					if (tableFlag) {
						tableStr = tableStr+"\t\t"+tmp+"\n";
					}
					if (tmp.contains(";")) {
						tableFlag = false;
						if (tableStr.contains("insert")) {
							tableNameList.add(tableName);
							tableSqlList.add(tableStr);
						}
						tableStr="";
					}
				}
				//注释结束
				if (tmp.trim().contains("*/")) {
					zsFlag = false;
				}
				
			}
		} catch (IOException e) {
			return e.getMessage();
		}
		try {
			br.close();
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (params.getFunc_full_name()==null || params.getFunc_full_name()=="") {
			return "目标表异常！";
		}
		tableMap.put("tableNameList", tableNameList);
		tableMap.put("tableSqlList", tableSqlList);
		if (tableNameList.size()!=tableSqlList.size()) {
			return "主体sql异常！";
		}
		//头部开始,返回头部文件
		String headFile="";
		try {
			headFile = SqlHead.sqlHead(params);
		} catch (Exception e) {
			return "头部文件出错！";
		}
		//参数定义
		String variableFile="";
		try {
			variableFile = SqlVariable.sqlVariable(params);
		} catch (Exception e) {
			return "参数文件出错！";
		}
		//主体
		String bodyFile="";
		try {
			bodyFile = SqlBody.sqlBody(params,tableMap);
		} catch (Exception e) {
			return "sql主体文件出错！";
		}
		//尾部结束
		String endFile="";
		try {
			endFile = SqlEnd.sqlEnd(params);
		} catch (Exception e) {
			return "尾部文件出错！";
		}
		
		//合并
		//最终sql文件
		String path = params.getOutPath()+File.separator+"p_"+params.getTable()+".sql";
		List<String> sqlPathList = new ArrayList<String>();
		sqlPathList.add(headFile);
		sqlPathList.add(variableFile);
		sqlPathList.add(bodyFile);
		sqlPathList.add(endFile);
		try {
			Utils.fileMerge(sqlPathList, path);
		} catch (Exception e) {
			return "文件合并异常！";
		}
		File outFileMlFile = new File(params.getOutPath()+File.separator+params.getOut_table());
		try {
			Utils.deleteFile(outFileMlFile);
		} catch (IOException e) {
			e.getMessage();
		}
		return "success!输出sql："+path;
	}
	/**
	 * 选择选项
	 * @param sc
	 */
	public static void out(Scanner sc) {
		System.out.println("请选择操作：\n"
				+ "1、执行sql生成 \n"
				+ "2、退出 \n");
	}
	
	
	
}
