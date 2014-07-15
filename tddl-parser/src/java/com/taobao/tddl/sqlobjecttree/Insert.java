package com.taobao.tddl.sqlobjecttree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.common.sqlobjecttree.Value;
import com.taobao.tddl.interact.sqljep.Comparative;

public class Insert extends DMLCommon{

	protected List<Object> valueObj=new ArrayList<Object>();
	
	protected Columns columns = new Columns();
	public void addColumn(String table, String column, String alias) {
		columns.addColumn(table, column, alias);
	}


	public void addColumnTandC(String tab, String col) {
		columns.addColumnTabAndCol(tab, col);
	}
	public Columns getColumns() {
		return columns;
	}

	public void setColumns(Columns columns) {
		this.columns = columns;
	}
	
	public void addValue(Object obj){
		this.valueObj.add(obj);
	}
	public List<Object> getValue(){
		return valueObj;
	}


	public Map<String, Comparative> getSubColumnsMap() {
		Map<String, Comparative> map=new HashMap<String, Comparative>();
		List<Column> cols=columns.getColumnsList();
		Comparable<?> temp=null;
		if(valueObj.size()!=cols.size()){
			throw new IllegalArgumentException("列名个数与insert参数不符");
		}
		for(int i=0;i<valueObj.size();i++){
			Column col=cols.get(i);
			Object val=valueObj.get(i);
			if(val instanceof Value){
				temp=((Value)val).eval();
				map.put(col.getColumn().toUpperCase(),new Comparative(Comparative.Equivalent,temp));
			}else if(val instanceof Comparable){
				map.put(col.getColumn().toUpperCase(), new Comparative(Comparative.Equivalent,(Comparable<?>)val));
			}
		}
		return map;
	}

	public void appendSQL(StringBuilder sb) {
		sb.append("INSERT INTO ");
		
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


	protected StringBuilder appendInsertBody(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb.append("INTO ");
		sb=super.regTableModifiable(oraTabName, list, sb);
		sb.append("(");
		sb=columns.regTableModifiable(oraTabName, list, sb);
		sb.append(")");
		sb.append(" VALUES ");
		sb=Utils.appendSQLListWithList(oraTabName, valueObj, sb, list);
		return sb;
	}


	protected void appendInsert(StringBuilder sb) {
	sb.append("INSERT ");
	}
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		
		sb.append(super.toString());
		sb.append("(");
		columns.appendSQL(sb);
		sb.append(") ").append("VALUES (");
		Utils.listToString(valueObj, sb);
		sb.append(")");
		return sb.toString();
	}
	public List<OrderByEle> nestGetOrderByList() {
		return Collections.emptyList();
	}

	@Override
	public WhereCondition getSubWhereCondition() {
		return null;
	}


	@Override
	public List<OrderByEle> nestGetGroupByList() {
		return Collections.emptyList();
	}

}
