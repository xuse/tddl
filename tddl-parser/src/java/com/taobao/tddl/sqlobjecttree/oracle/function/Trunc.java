package com.taobao.tddl.sqlobjecttree.oracle.function;

import com.taobao.tddl.sqlobjecttree.common.value.OperationBeforOneOrTwoArgsFunction;

public class Trunc extends OperationBeforOneOrTwoArgsFunction{

	@Override
	public String getFuncName() {
		return "TRUNC";
	}


}
