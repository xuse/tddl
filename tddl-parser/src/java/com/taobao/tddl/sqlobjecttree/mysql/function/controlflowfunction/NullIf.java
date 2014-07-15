package com.taobao.tddl.sqlobjecttree.mysql.function.controlflowfunction;

import com.taobao.tddl.sqlobjecttree.common.value.OperationBeforTwoArgsFunction;

public class NullIf extends OperationBeforTwoArgsFunction{

	public String getFuncName() {
		return "nullif";
	}

}
