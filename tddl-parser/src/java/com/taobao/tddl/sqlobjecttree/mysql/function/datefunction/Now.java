package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import java.util.Date;
import java.util.List;

import com.taobao.tddl.sqlobjecttree.common.value.NoArgFunction;

public class Now extends NoArgFunction{

	@Override
	public String getFuncName() {
		return "now";
	}

	@Override
	public Comparable<?> getVal(List<Object> args) {
		return new Date();
	}
}
