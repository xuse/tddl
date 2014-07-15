package com.taobao.tddl.sqlobjecttree.mysql;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.Update;
import com.taobao.tddl.sqlobjecttree.WhereCondition;


public class MyUpdate extends Update {
	private RangeSelector limit = null;
	public MyUpdate() {
		super();
		limit=new RangeSelector((MyWhereCondition)where);
	}
	public WhereCondition getWhereCondition() {
		return new MyWhereCondition();
	}
	public void appendSQL(StringBuilder sb) {
		super.appendSQL(sb);
		limit.appendSQL(sb);
	}
	public int getSkip(List<Object> param) {
		int temp=DEFAULT_SKIP_MAX;
		int skip=super.getSkip(param);
		temp=limit.getSkip(param);
		if(temp>skip){
			skip=temp;
		}
		return skip;
	}
	protected int getRangeOrMax(List<Object> param) {
		int temp=DEFAULT_SKIP_MAX;
		int max=super.getRangeOrMax(param);
		temp=limit.getRange(param);
		if(temp>max){
			max=temp;
		}
		return max;
	}
	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb=super.regTableModifiable(oraTabName, list, sb);
		sb=limit.regTableModifiable(oraTabName, list, sb);
		return sb;
	}
}
