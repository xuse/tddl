package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import com.taobao.tddl.sqlobjecttree.common.value.OperationBeforTwoArgsFunction;

public class DateAdd extends OperationBeforTwoArgsFunction{
	public String getFuncName() {
		return "date_add";
	}
	
//	/**
//	 * 默认超类里面第一个参数为时间
//	 * 第二个参数为Interval 函数
//	 */
//	public Comparable<?> getVal(List<Object> args) {
//		Calendar cal=Calendar.getInstance();
//		cal.setTime((java.util.Date)arg1);
//		Interval temp=(Interval)arg2;
//		cal.add((Integer)temp.dateUnit.getVal(args), temp.expr);
//	    return cal;
//	}
}
