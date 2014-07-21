package com.taobao.tddl.sqlobjecttree.oracle.function;

import com.taobao.tddl.sqlobjecttree.common.value.OperationBeforTwoArgsFunction;

/*
 * @author guangxia
 * @since 1.0, 2009-10-27 下午05:14:04
 */
public class OracleBitAnd extends OperationBeforTwoArgsFunction {

	@Override
	public String getFuncName() {
		return "BITAND";
	}

}
