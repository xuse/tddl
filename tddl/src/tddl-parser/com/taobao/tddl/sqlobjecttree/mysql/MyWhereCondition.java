package com.taobao.tddl.sqlobjecttree.mysql;

import com.taobao.tddl.sqlobjecttree.WhereCondition;


public class MyWhereCondition extends WhereCondition {
	private Object start =null;
	private Object range = null;
	public Object getStart() {
		return start;
	}
	public Object getRange() {
		return range;
	}
	public void setRange(Object range) {
		this.range = range;
	}
	public void setStart(Object start) {
		this.start = start;
	}
	


}
