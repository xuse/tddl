package com.taobao.tddl.sqlobjecttree.common;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.SQLFragment;
import com.taobao.tddl.sqlobjecttree.JoinClause;
import com.taobao.tddl.sqlobjecttree.Select;
import com.taobao.tddl.sqlobjecttree.TableName;
import com.taobao.tddl.sqlobjecttree.Utils;


/**
 * 也就是一个出现在table栏位的子查询
 * @author shenxun
 *
 */
public class TableNameSubQueryImp implements TableName{
	private String alias;
	private Select subSelect;
	private boolean isOracle;
	
	public TableNameSubQueryImp() {}
	public TableNameSubQueryImp(boolean isOracle) {
		this.isOracle = isOracle;
	}
	public void setJoinClause(JoinClause joinClause) {
		if(joinClause != null){
			throw new IllegalArgumentException(" not support join in subquery ");
		}
	}
	public String getAlias() {
		return alias;
	}
	
	public Select getSubSelect() {
		return subSelect;
	}

	public void setSubSelect(Select subSelect) {
		this.subSelect = subSelect;
	}

	public void appendSQL(StringBuilder sb) {
		Utils.appendSQLList(subSelect,sb);
		if(alias!=null){
			sb.append(" ");
			sb.append(alias);
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(subSelect.toString());
		if(alias!=null){
			sb.append(isOracle ? " " : " AS ");
			sb.append(alias);
		}
		return sb.toString();
	}

	public void setAlias(String alias) {
		this.alias=alias;
		
	}

	public Set<String> getTableName() {
		throw new IllegalArgumentException("should not be here");
	}


//	public void regTableModifiable(String oraTabName,
//			List<ModifiableTableName> list) {
//		subSelect.regTableModifiable(oraTabName, list);
//	}

	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
			sb=Utils.appendSQLListWithList(oraTabName, subSelect, sb, list);
			if(alias!=null){
				sb.append(isOracle ? " " : " as ");
				sb.append(alias);
		}
			return sb;
	}

	public void appendAliasToSQLMap(Map<String, SQLFragment> map) {
		getSubSelect().buildAliasToTableAndColumnMapping(map);
		if(getAlias()!=null){
			map.put(getAlias().toUpperCase(), this);
		}
	}

	
	
}
