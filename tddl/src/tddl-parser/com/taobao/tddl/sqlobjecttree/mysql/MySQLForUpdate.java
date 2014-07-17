package com.taobao.tddl.sqlobjecttree.mysql;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.SelectUpdate;

public class MySQLForUpdate implements SelectUpdate{

	public void appendSQL(StringBuilder sb) {
		sb.append(" FOR UPDATE");
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb.append(" FOR UPDATE");
		return sb;
	}
}
