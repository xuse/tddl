package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import java.util.List;

import com.taobao.tddl.sqlobjecttree.Utils;
import com.taobao.tddl.sqlobjecttree.common.value.OneArgFunction;
import com.taobao.tddl.sqlobjecttree.mysql.MySQLFunctionUtils;

public class FromDays extends OneArgFunction{

	public String getFuncName() {
		return "from_days";
	}

	public Comparable<?> getVal(List<Object> args) {
		Comparable<?> days=Utils.getVal(args, arg1);
		if(days instanceof Integer){
			return MySQLFunctionUtils.fromDays((Integer)days);
		}else{
			throw new IllegalArgumentException("from days 只支持int型的参数");
		}
	}
}
