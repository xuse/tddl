package com.taobao.tddl.sqlobjecttree.common.expression;

import com.taobao.tddl.interact.sqljep.Comparative;

public class Equivalent extends ComparableExpression {

	protected int getComparativeOperation() {
		return Comparative.Equivalent;
	}

	public String getRelationString() {
		return " = ";
	}


}
