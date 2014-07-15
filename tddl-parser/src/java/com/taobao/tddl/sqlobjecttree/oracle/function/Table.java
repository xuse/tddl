package com.taobao.tddl.sqlobjecttree.oracle.function;

import java.util.List;

import com.taobao.tddl.sqlobjecttree.Utils;
import com.taobao.tddl.sqlobjecttree.common.value.OneArgFunction;

public class Table extends OneArgFunction {

	@Override
	public String getFuncName() {
		return "TABLE";
	}

	@Override
	public Comparable<?> getVal(List<Object> args) {
		return Utils.getVal(args,arg1);
	}
}
