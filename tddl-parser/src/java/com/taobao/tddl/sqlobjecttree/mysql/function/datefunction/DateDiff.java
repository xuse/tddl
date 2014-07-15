package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import com.taobao.tddl.sqlobjecttree.common.value.OperationBeforTwoArgsFunction;

public class DateDiff extends OperationBeforTwoArgsFunction{
	public String getFuncName() {
		return "datediff";
	}

}
