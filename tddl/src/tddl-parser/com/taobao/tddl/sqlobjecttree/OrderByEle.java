package com.taobao.tddl.sqlobjecttree;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.SQLFragment;




public class OrderByEle implements Cloneable,SQLFragment{
	private String table;
	private String name;
	private boolean isASC;
	public OrderByEle(String table,String name,boolean isASC) {
		this.table=table;
		this.name=name;
		this.isASC=isASC;
	}
	public String getTable() {
		return table;
	}
	public String getName() {
		return name;
	}
	public boolean isASC() {
		return isASC;
	}
	public Object clone()throws CloneNotSupportedException{
		return super.clone();
	}

	public void appendSQL(StringBuilder sb) {
		if (table!= null) {
			sb.append(table).append(".");
		}
		sb.append(name);
		if (isASC) {
			sb.append(" ASC ");
		} else {
			sb.append(" DESC ");
		}		
	}
	public StringBuilder regTableModifiable(Set<String> logicTableNames, List<Object> list,
			StringBuilder sb) {
		if(table!=null){
			if(logicTableNames.contains(table)){
				if(sb!=null){
					list.add(sb.toString());
					TableWrapper wr=new TableWrapper();
					wr.setOriTable(table);
					list.add(wr);
					sb=new StringBuilder();
					sb.append(".");
				}else{
					throw new IllegalStateException("wrong state");
				}
			}else{
				sb.append(table).append(".");
			}
		}
		sb.append(name);
		if (isASC) {
			sb.append(" ASC ");
		} else {
			sb.append(" DESC ");
		}
			return sb;
		}		
}
