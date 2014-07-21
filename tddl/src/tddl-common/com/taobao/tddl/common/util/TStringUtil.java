package com.taobao.tddl.common.util;

import java.util.ArrayList;
import java.util.List;

/**
 * TDDL专用的字符处理便捷类
 * 
 * @author linxuan
 * 
 */
public class TStringUtil {
//	public static void main(String[] args) {
//		System.out.println(getBetween("/*+dsKey= dbc */", "/*+dsKey=", "*/"));
//	}

	/**
	 * 获得第一个start，end之间的字串， 不包括start，end本身。返回值已做了trim
	 */
	public static String getBetween(String sql, String start, String end) {
		int index0 = sql.indexOf(start);
		if (index0 == -1) {
			return null;
		}
		int index1 = sql.indexOf(end, index0);
		if (index1 == -1) {
			return null;
		}
		return sql.substring(index0 + start.length(), index1).trim();
	}

	/**
	 * 只做一次切分
	 * @param str
	 * @param splitor
	 * @return
	 */
	public static String[] twoPartSplit(String str, String splitor) {
		if (splitor != null) {
			int index = str.indexOf(splitor);
			if(index!=-1){
			    String first = str.substring(0, index);
			    String sec = str.substring(index + splitor.length());
		        return new String[]{first,sec};
			}else{
				return new String[] { str };
			}
		} else {
			return new String[] { str };
		}
	}
	
	public static List<String> split(String str,String splitor){
		List<String> re=new ArrayList<String>();
		String[] strs=twoPartSplit(str,splitor);
		if(strs.length==2){
			re.add(strs[0]);
			re.addAll(split(strs[1],splitor));
		}else{
			re.add(strs[0]);
		}
		return re;
	}
	
	public static void main(String[] args){
		String test="sdfsdfsdfs liqiangsdfsdfwerfsdfliqiang woshi whaosdf";
		List<String> strs=split(test,"liqiang");
		for(String str:strs){
			System.out.println(str);
		}
	}
	
	/**
	 * 去除第一个start,end之间的字符串，包括start,end本身
	 * 
	 * @param sql
	 * @param start
	 * @param end
	 * @return
	 */
	public static String removeBetweenWithSplitor(String sql, String start,
			String end) {
		int index0 = sql.indexOf(start);
		if (index0 == -1) {
			return sql;
		}
		int index1 = sql.indexOf(end, index0);
		if (index1 == -1) {
			return sql;
		}
		StringBuilder sb = new StringBuilder();
		sb.append(sql.substring(0, index0));
		sb.append(" ");
		sb.append(sql.substring(index1 + end.length()));
		return sb.toString();
	}
	
	public static String removeBetweenWithSplitorNotExistNull(String sql, String start,
			String end){
		int index0=sql.indexOf(start);
		if(index0==-1){
			return null;
		}
		int index1 = sql.indexOf(end, index0);
		if(index1==-1){
			return null;
		}
		StringBuilder sb=new StringBuilder();
		sb.append(sql.substring(0,index0));
		sb.append(" ");
		sb.append(sql.substring(index1+end.length()));
		return sb.toString();
	}

	/**
	 * 将所有/t/s/n等空白符全部替换为空格，并且去除多余空白 各种不同实现的比较测试，参见：TStringUtilTest
	 */
	public static String fillTabWithSpace(String str) {
		if (str == null) {
			return null;
		}

		str = str.trim();
		int sz = str.length();
		StringBuilder buffer = new StringBuilder(sz);

		int index = 0, index0 = -1, index1 = -1;
		for (int i = 0; i < sz; i++) {
			char c = str.charAt(i);
			if (!Character.isWhitespace(c)) {
				if (index0 != -1) {
					// if (!(index0 == index1 && str.charAt(i - 1) == ' ')) {
					if (index0 != index1 || str.charAt(i - 1) != ' ') {
						buffer.append(str.substring(index, index0)).append(" ");
						index = index1 + 1;
					}
				}
				index0 = index1 = -1;
			} else {
				if (index0 == -1) {
					index0 = index1 = i; // 第一个空白
				} else {
					index1 = i;
				}
			}
		}

		buffer.append(str.substring(index));

		return buffer.toString();
	}
}
