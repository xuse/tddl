package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.Function;

public class MySQLDateStringWrapperFunction implements Function{
	String date="";
	private DateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private DateFormat df10=new SimpleDateFormat("yyyy-MM-dd");
	public String getNestedColName() {
		return date;
	}

	public MySQLDateStringWrapperFunction(String date) {
		this.date=date;
	}
	
	public void setValue(List<Object> values) {
		throw new IllegalStateException("should not be here");
	}

	public Comparable<?> eval() {
		return this;
	}

	public Comparable<?> getVal(List<Object> args) {
		Date d=null;
		try {
			if (date.endsWith(" null")) {
				date = date.substring(0, date.length() - 5);
			}
			if (date.length() == 10) {
				d = df10.parse(date);
			} else {
				d = df.parse(date);
			}
		} catch (ParseException e) {
			throw new IllegalArgumentException("tddl不能正确的识别mysql日期,当前系统记录的日期是"+date);
		}
		return d;
	}

	public void appendSQL(StringBuilder sb) {
		sb.append("'").append(date).append("'");
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb.append("'").append(date).append("'");
		return sb;
	}

	public int compareTo(Object arg0) {
		throw new IllegalStateException("should not be here");
	}

}
