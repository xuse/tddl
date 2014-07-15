package com.taobao.tddl.sqlobjecttree;

import java.util.List;
import java.util.Set;

/**
 * @author junyu
 * 
 */
public class HavingCondition extends WhereCondition {
	@Override
	public void appendSQL(StringBuilder sb) {
		StringBuilder temp = new StringBuilder();
		super.getExpGroup().appendSQL(temp);
		if (temp.length() != 0) {
			sb.append(" HAVING ");
			sb.append(temp);
		}
	}

	@Override
	public StringBuilder regTableModifiable(Set<String> logicTableNames,
			List<Object> list, StringBuilder sb) {
		if(super.getExpGroup().getExpressions().size()!=0){
			sb.append(" HAVING ");
			sb=super.getExpGroup().regTableModifiable(logicTableNames, list, sb);
		}
		return sb;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		StringBuilder temp=new StringBuilder();
		temp.append(super.getExpGroup().toString());
		if(temp.length()!=0){
			sb.append(" HAVING ");
			sb.append(temp);
		}
		return sb.toString();
	}
}
