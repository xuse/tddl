package com.taobao.tddl.client.user.method;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2012-3-10下午01:23:38
 */
public class TddlUserMethod {
	
	/**
	 * 字符串到date类型转换
	 * @param dateString
	 * @param dateFormat
	 * @return
	 * @throws ParseException
	 */
	public static Date stringToDate(String dateString, String dateFormat) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		Date date = sdf.parse(dateString);
		return date;
	}
	
	public static String dateToString(Date date, String dateFormat) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		String dateString = sdf.format(date);
		return dateString;
	}

	public static void main(String[] args) {
		String dateString = "20120201";
		String dateFormat = "yyyyMMdd";
		
		Date date = null;
		try {
			date = TddlUserMethod.stringToDate(dateString, dateFormat);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(date);
		
	}
}
