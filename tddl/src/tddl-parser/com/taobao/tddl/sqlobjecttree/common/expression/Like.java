package com.taobao.tddl.sqlobjecttree.common.expression;

import com.taobao.tddl.interact.sqljep.Comparative;

public class Like extends ComparableExpression {

	protected int getComparativeOperation() {
		return Comparative.Like;
	}

	public String getRelationString() {
		return " like ";
	}


}
