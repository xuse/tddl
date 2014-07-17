package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import java.util.Date;
import java.util.List;

import com.taobao.tddl.sqlobjecttree.common.value.NoArgFunction;

public class Sysdate extends NoArgFunction{
	@Override
	public String getFuncName() {
		return "sysdate";
	}

	@Override
	public Comparable<?> getVal(List<Object> args) {
		return new Date();
	}
}
