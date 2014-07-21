package com.taobao.tddl.parser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 使用约束
 * 1. 不考虑sql中引号字符串包含关键字的情况。假定sql中无字串。都用prepareStatement加问号方式
 * 2. 对于from后有两个表的，包括括号方式的临时视图，伪表之类，都以第一个表名为准
 * 
 * @author linxuan
 *
 */
public class SQLPreParser {

	private static Pattern p = Pattern.compile("\\s+([a-z0-9_$]+)\\s+");
	private static Pattern pinto = Pattern.compile("\\s+into\\s+([a-z0-9_$]+)[\\s(]+");
	private static Pattern pfrom =        Pattern.compile("\\s+from\\s+([a-z0-9_$]+)\\s+");
	private static Pattern pselect_from = Pattern.compile("\\s+from\\s+([a-z0-9_$]+)[\\s)]+");

	public static String findTableName(String sql0) {
		if (sql0 == null)
			return null;
		sql0 = sql0.trim(); //trim可以去掉\\s,包括换行符、制表符等
		if (sql0.length() < 7) {
			return null;
		}

		//TODO 去除hint
		sql0 = sql0.toLowerCase();
		sql0 = sql0 + " "; //便于处理

		if (sql0.startsWith("update")) {
			Matcher m = p.matcher(sql0);
			if (m.find(6)) {
				return m.group(1);
			}
			return null;
		}

		if (sql0.startsWith("delete")) {
			Matcher m = pfrom.matcher(sql0);
			if (m.find(6)) {
				return m.group(1);
			}
			return null;
		}

		if (sql0.startsWith("insert")) {
			Matcher m = pinto.matcher(sql0);
			if (m.find(6)) {
				return m.group(1);
			}
			return null;
		}

		if (!sql0.startsWith("select")) {
			return null; //不以update delete select开头的sql
		}

		Matcher m = pselect_from.matcher(sql0);
		if (m.find(6)) {
			return m.group(1);
		}

		return null;
	}

	public static void main(String[] args) throws IOException {
		System.out.println(findTableName("	\r	\r\n \n   	update 	t_a$ble0 set a=1"));
		System.out.println(findTableName("delete from t_a$ble0\r\n t where t.id = 0"));
		System.out.println(findTableName("delete from t_a$ble0"));
		System.out.println(findTableName("insert into t_a$ble0 t values(?,?) where t.id = 0"));
		System.out.println(findTableName("insert into t_a$ble0(col_a, col_b) values(?,?) where id = 0"));
		System.out.println(findTableName("select count(*) from t_a$ble0"));
		System.out.println(findTableName("select 1 from t_a$ble0 t where t.id=0"));
		System.out.println(findTableName("select 1 from (select id from table2) t where t.id = 5"));
		System.out.println(findTableName("select 1 from(select id from table2) t where t.id = 5"));
		System.out.println(findTableName("select 1 from (select id from table2) t, t_a$ble0 a where t.id = a.id"));
		System.out.println(findTableName("select 1 from t_a$ble0 a, (select id from table2) t where t.id = a.id"));
		System.out.println(findTableName("select count(*) from CRM_KNOWLEDGE_DETAIL kc,CRM_KNOWLEDGE_BASE a where a.id=kc.KNOWLEDGE_ID"));
		System.out.println(findTableName("SELECT * FROM (SELECT CAST(STR2NUMLIST(#in#) AS NUMTABLETYPE) FROM dual) WHERE rownum <= 200"));
	}

	public static void main2(String[] args) throws IOException {
		String filePath = "";
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
		String line;
		while ((line = br.readLine()) != null) {
			System.out.println(findTableName(line) + " <-- " + line);
		}
	}
}
