package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.sqlobjecttree.Function;

public class MySQLDateFuctionMap {
	public final static MySQLDateFuctionMap reg=new MySQLDateFuctionMap();
	private  final static Map<String, Class<? extends Function>> funcMap=new HashMap<String, Class<? extends Function>>();
	
	static{
		funcMap.put("NOW", Now.class);
		funcMap.put("SYSDATE", Sysdate.class);
		funcMap.put("FROM_DAYS", FromDays.class);
		funcMap.put("TO_DAYS", ToDays.class);
		funcMap.put("CURDATE", Curdate.class);
		funcMap.put("CURTIME", CurTime.class);
		funcMap.put("ADDDATE", AddDate.class);
		funcMap.put("CONVERT_TZ", Convert_tz.class);
		funcMap.put("DATEADD", DateAdd.class);
		funcMap.put("DATEDIFF", DateDiff.class);
		funcMap.put("DAYOFMONTH", DayOfMonth.class);
		funcMap.put("YEAR", Year.class);
		funcMap.put("DAYOFYEAR", DayOfYear.class);
		funcMap.put("MONTH", Month.class);
	}
	
	public boolean containsKey(String key){
		return  funcMap.containsKey(key);
	}
	public Function get(String key){
		Function cls=null;
		try {
			if(key==null||key.trim().equals("")){
				throw new IllegalArgumentException("group function²»ÄÜÎª¿Õ");
			}
			cls=funcMap.get(key.toUpperCase()).newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);	
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);	
		}
		return cls;
	}
	
}
