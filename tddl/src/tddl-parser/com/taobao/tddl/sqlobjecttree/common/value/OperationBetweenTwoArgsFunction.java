package com.taobao.tddl.sqlobjecttree.common.value;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.Utils;


public abstract class OperationBetweenTwoArgsFunction extends OperationBeforTwoArgsFunction{
	public void appendSQL(StringBuilder sb) {
		sb.append("(");
		Utils.appendSQLList(arg1, sb);
		sb.append(" ").append(getFuncName()).append(" ");
		Utils.appendSQLList(arg2, sb);
		sb.append(")");
	}
	@Override
	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb.append("(");
		sb=Utils.appendSQLListWithList(oraTabName, arg1, sb, list);
		sb.append(" ").append(getFuncName()).append(" ");
		sb=Utils.appendSQLListWithList(oraTabName, arg2, sb, list);
		sb.append(")");
		return sb;
	}
}
