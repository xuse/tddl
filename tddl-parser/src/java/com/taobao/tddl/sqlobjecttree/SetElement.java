package com.taobao.tddl.sqlobjecttree;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.common.sqlobjecttree.SQLFragment;





public class SetElement implements SQLFragment{
	public Column col;
	public Object value;	
	public void appendSQL(StringBuilder sb) {
		Utils.appendSQLList(col, sb);
		sb.append("=");
		Utils.appendSQLList(value, sb);
	
	}
//	public void regTableModifiable(String oraTabName,
//			List<ModifiableTableName> list) {
//		col.regTableModifiable(oraTabName, list);
//		ModifiedTableUtils.regModifiedTab(value, oraTabName, list);
//	}
	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		sb=Utils.appendSQLListWithList(oraTabName,col, sb, list);
		sb.append("=");
		sb=Utils.appendSQLListWithList(oraTabName, value, sb, list);
		return sb;
	}
}
