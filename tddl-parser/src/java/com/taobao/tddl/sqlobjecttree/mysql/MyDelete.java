package com.taobao.tddl.sqlobjecttree.mysql;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.Delete;
import com.taobao.tddl.sqlobjecttree.WhereCondition;


public class MyDelete extends Delete {
	private RangeSelector limit = null;

	public MyDelete() {
		super();
		limit = new RangeSelector((MyWhereCondition)where);
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
	@Override
	protected WhereCondition getWhereCondition() {
		return new MyWhereCondition();
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
	public void appendSQL(StringBuilder sb) {
		super.appendSQL(sb);
		limit.appendSQL(sb);
	}
	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb=super.regTableModifiable(oraTabName, list, sb);
		sb=limit.regTableModifiable(oraTabName, list, sb);
		return sb;
	}
}
