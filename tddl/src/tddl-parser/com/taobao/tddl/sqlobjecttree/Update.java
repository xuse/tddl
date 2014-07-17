package com.taobao.tddl.sqlobjecttree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.sqlobjecttree.common.ColumnImp;
import com.taobao.tddl.sqlobjecttree.common.expression.ExpressionGroup;

public class Update extends DMLCommon {
	protected WhereCondition where = null;
	protected List<SetElement> setElements=new ArrayList<SetElement>();
	
	public void addSetElement(String colName,String table,Object obj){
		SetElement set=new SetElement();
		set.col=new ColumnImp(table,colName,null);
		set.value=obj;
		setElements.add(set);
	}
	public List<SetElement> getSetElements(){
		return setElements;
	}
	public Update() {
		where=getWhereCondition();
		where.setHolder(holder);
	}
	
	protected WhereCondition getWhereCondition(){
		return new WhereCondition();
	}
	public WhereCondition getWhere() {
		return where;
	}

	public void addAndWhereExpressionGroup(ExpressionGroup exp) {
		where.addAndExpression(exp);
	}

	public void appendSQL(StringBuilder sb) {
		appendUpdate(sb);
		super.appendSQL(sb);
		sb.append("SET ");
		boolean comma=false;
		for(SetElement ele:setElements){
			if(comma){
				sb.append(",");
			}
			comma=true;
			ele.appendSQL(sb);
		}
		where.appendSQL(sb);
		
	}
	public Map<String, Comparative> getSubColumnsMap() {
		return where.eval();
	}
	public String toString() {
		StringBuilder sb = new StringBuilder();
		appendUpdate(sb);
		sb.append(super.toString());
		sb.append("SET ");
		boolean comma=false;
		for(SetElement ele:setElements){
			if(comma){
				sb.append(",");
			}
			comma=true;
			ele.appendSQL(sb);
		}
		sb.append(where.toString());
		return sb.toString();
	}
	public List<OrderByEle> nestGetOrderByList() {
		return where.getOrderByColumns();
	}
//	public void regTableModifiable(String oraTabName,
//			List<ModifiableTableName> list) {
//		super.regTableModifiable(oraTabName, list);
//		for(SetElement ele:setElements){
//			ele.regTableModifiable(oraTabName, list);
//		}
//		where.regTableModifiable(oraTabName, list);
//	}

	@Override
	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		appendUpdate(sb);
		return appendUpdateBody(oraTabName, list, sb);
	}
	protected StringBuilder appendUpdateBody(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb=super.regTableModifiable(oraTabName, list, sb);
		sb.append("SET ");
		boolean comma=false;
		for(SetElement ele:setElements){
			if(comma){
				sb.append(",");
			}
			comma=true;
			sb=ele.regTableModifiable(oraTabName, list, sb);
		}
		list.add(sb.toString());
		sb=new StringBuilder();
		ReplacableWrapper replace= new VersionWrapper();
		list.add(replace);
		sb=where.regTableModifiable(oraTabName, list, sb);
		return sb;
	}
	protected void appendUpdate(StringBuilder sb) {
		sb.append("UPDATE ");
	}
	
	@Override
	public WhereCondition getSubWhereCondition() {
		return where;
	}
	@Override
	public List<OrderByEle> nestGetGroupByList() {
		return Collections.emptyList();
	}
}
