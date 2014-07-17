package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import java.util.Date;
import java.util.List;

import com.taobao.tddl.sqlobjecttree.common.value.Constant;

public class Current_date extends Constant{
	@Override
	public String getFuncName() {
		return "current_date";
	}

	public Comparable<?> getVal(List<Object> args) {
		return new Date();
	}
}
