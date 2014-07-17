package com.taobao.tddl.sqlobjecttree.mysql.function.stringfunction;

import com.taobao.tddl.sqlobjecttree.common.value.OperationBeforTwoArgsFunction;

public class Concat extends OperationBeforTwoArgsFunction {
	//后加的，因为有个版本V810没有合并到主干
	@Override
	public String getFuncName() {
		return "CONCAT";
	}
}