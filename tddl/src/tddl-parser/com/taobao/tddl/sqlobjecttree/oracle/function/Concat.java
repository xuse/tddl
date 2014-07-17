package com.taobao.tddl.sqlobjecttree.oracle.function;

import com.taobao.tddl.sqlobjecttree.common.value.OperationBeforTwoArgsFunction;

public class Concat extends OperationBeforTwoArgsFunction {

	@Override
	public String getFuncName() {
		return "CONCAT";
	}

}

