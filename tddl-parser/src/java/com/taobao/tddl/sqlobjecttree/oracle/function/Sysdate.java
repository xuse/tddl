package com.taobao.tddl.sqlobjecttree.oracle.function;

import java.util.Date;
import java.util.List;

import com.taobao.tddl.sqlobjecttree.common.value.Constant;

public class Sysdate extends Constant {

	@Override
	public String getFuncName() {
		return "SYSDATE";
	}

	public Comparable<?> getVal(List<Object> args) {
		return new Date();
	}

	

}
