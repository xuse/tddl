package com.taobao.tddl.sqlobjecttree.mysql;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.SelectUpdate;

public class MySQLLockInShareMode implements SelectUpdate{

	public void appendSQL(StringBuilder sb) {
		sb.append(" LOCK IN SHARE MODE");
		
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb.append(" LOCK IN SHARE MODE");
		return sb;
	}

}
