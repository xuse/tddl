package com.taobao.tddl.sqlobjecttree.common.expression;

import com.taobao.tddl.sqlobjecttree.Utils;


public abstract class TwoArgsExpressionWithParen extends ComparableExpression{
	public void appendSQL(StringBuilder sb) {
		Utils.appendSQLList(left, sb);
		sb.append(getRelationString());
		sb.append("(");
		Utils.appendSQLList(right, sb);
		sb.append(")");
	}
}
