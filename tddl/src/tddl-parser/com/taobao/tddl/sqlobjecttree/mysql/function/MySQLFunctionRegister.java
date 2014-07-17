package com.taobao.tddl.sqlobjecttree.mysql.function;


import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.sqlobjecttree.Function;
import com.taobao.tddl.sqlobjecttree.common.value.function.Avg;
import com.taobao.tddl.sqlobjecttree.common.value.function.Count;
import com.taobao.tddl.sqlobjecttree.common.value.function.Max;
import com.taobao.tddl.sqlobjecttree.common.value.function.Min;
import com.taobao.tddl.sqlobjecttree.common.value.function.Sum;
import com.taobao.tddl.sqlobjecttree.mysql.function.controlflowfunction.If;
import com.taobao.tddl.sqlobjecttree.mysql.function.controlflowfunction.IfNull;
import com.taobao.tddl.sqlobjecttree.mysql.function.controlflowfunction.NullIf;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.AddDate;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.Convert_tz;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.CurTime;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.Curdate;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.DateAdd;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.DateDiff;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.DayOfMonth;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.DayOfYear;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.FromDays;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.Interval;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.Month;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.Now;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.StrToDate;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.Sysdate;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.ToDays;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.Year;
import com.taobao.tddl.sqlobjecttree.mysql.function.stringfunction.Concat;
import com.taobao.tddl.sqlobjecttree.mysql.function.stringfunction.STRCMP;


public class MySQLFunctionRegister {
	public final static MySQLFunctionRegister reg=new MySQLFunctionRegister();
	private  final static Map<String, Class<? extends Function>> funcReg=new HashMap<String, Class<? extends Function>>();
	static{
		//control
		funcReg.put("IFNULL", IfNull.class);
		funcReg.put("IF", If.class);
		funcReg.put("NULLIF", NullIf.class);
		//group 
		funcReg.put("COUNT", Count.class);
		funcReg.put("MAX", Max.class);
		funcReg.put("MIN", Min.class);
		funcReg.put("AVG", Avg.class);
		funcReg.put("SUM", Sum.class);
		//date
		funcReg.put("NOW", Now.class);
		funcReg.put("SYSDATE", Sysdate.class);
		funcReg.put("FROM_DAYS", FromDays.class);
		funcReg.put("TO_DAYS", ToDays.class);
		funcReg.put("CURDATE", Curdate.class);
		funcReg.put("CURTIME", CurTime.class);
		funcReg.put("ADDDATE", AddDate.class);
		funcReg.put("CONVERT_TZ", Convert_tz.class);
		funcReg.put("DATEADD", DateAdd.class);
		funcReg.put("DATEDIFF", DateDiff.class);
		funcReg.put("DAYOFMONTH", DayOfMonth.class);
		funcReg.put("YEAR", Year.class);
		funcReg.put("DAYOFYEAR", DayOfYear.class);
		funcReg.put("MONTH", Month.class);
		funcReg.put("STR_TO_DATE", StrToDate.class);
		
		//str
		funcReg.put("STRCMP", STRCMP.class);
		//后加的，因为有个版本V810没有合并到主干
		funcReg.put("CONCAT", Concat.class);
		funcReg.put("INTERVAL",Interval.class);
	}
	public boolean containsKey(String key){
		return  funcReg.containsKey(key);
	}
	public Function get(String key){
		Function cls=null;
		try {
			if(key==null||key.trim().equals("")){
				throw new IllegalArgumentException("group function不能为空");
			}
			cls=funcReg.get(key.toUpperCase()).newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);	
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);	
		}
		return cls;
	}
	
}
