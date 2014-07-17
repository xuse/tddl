package com.taobao.tddl.sqlobjecttree;

import java.util.List;
import java.util.Set;

public class Replace extends Insert{
	public void appendSQL(StringBuilder sb) {
		sb.append("REPLACE INTO ");
		
		super.appendSQL(sb);
		sb.append("(");
		columns.appendSQL(sb);
		sb.append(")");
		sb.append(" ").append("VALUES ");
		Utils.appendSQLList(valueObj, sb);
		
	}
	@Override
	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		appendInsert(sb);
		return appendInsertBody(oraTabName, list, sb);
	}
	protected void appendInsert(StringBuilder sb) {
	sb.append("REPLACE ");
	}
}
