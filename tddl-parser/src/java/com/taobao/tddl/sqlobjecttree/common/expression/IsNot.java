package com.taobao.tddl.sqlobjecttree.common.expression;

import com.taobao.tddl.interact.sqljep.Comparative;

public class IsNot extends ComparableExpression {

	protected int getComparativeOperation() {
		return Comparative.NotEquivalent;
	}

	public String getRelationString() {
		return " is not ";
	}


}
