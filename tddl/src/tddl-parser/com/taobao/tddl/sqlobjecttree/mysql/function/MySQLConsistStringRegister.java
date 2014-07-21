package com.taobao.tddl.sqlobjecttree.mysql.function;


import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.sqlobjecttree.Function;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.Current_date;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.Sysdate;
import com.taobao.tddl.sqlobjecttree.mysql.function.interval.datetype.Day;
import com.taobao.tddl.sqlobjecttree.mysql.function.interval.datetype.Hour;
import com.taobao.tddl.sqlobjecttree.mysql.function.interval.datetype.IntervalMonth;
import com.taobao.tddl.sqlobjecttree.mysql.function.interval.datetype.IntervalYear;
import com.taobao.tddl.sqlobjecttree.mysql.function.interval.datetype.Minute;
import com.taobao.tddl.sqlobjecttree.mysql.function.interval.datetype.Second;


public class MySQLConsistStringRegister {
	public final static MySQLConsistStringRegister reg=new MySQLConsistStringRegister();
	private  final static Map<String, Class<? extends Function>> consistReg=new HashMap<String, Class<? extends Function>>();
	static{
		consistReg.put("SYSDATE", Sysdate.class);
		consistReg.put("CURRENT_DATE", Current_date.class);
		/**
		 * ADD BY JUNYU
		 */
		consistReg.put("DAY",Day.class);
		consistReg.put("SECOND",Second.class);
		consistReg.put("HOUR",Hour.class);
		consistReg.put("MINUTE", Minute.class);
		
		/**
		 * 这个会导致规则冲突，因为YEAR(),MONTH()函数
		 * 与INTERVAL的YEAR,MONTH文本一致，但是ANTLR
		 * 的look ahead特性可以解决这个冲突，因为YEAR
		 * 函数后面出现的符号是(,INTERVAL的YEAR后不可能
		 * 出现这个符号，反之亦然
		 */
		consistReg.put("YEAR", IntervalYear.class);
		consistReg.put("MONTH", IntervalMonth.class);
	}
	public boolean containsKey(String key){
		return  consistReg.containsKey(key);
	}
	public Function get(String key){
		Function cls=null;
		try {
			if(key==null||key.trim().equals("")){
				throw new IllegalArgumentException("group function不能为空");
			}
			cls=consistReg.get(key.toUpperCase()).newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);	
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);	
		}
		return cls;
	}
	
}
