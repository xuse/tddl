package com.taobao.tddl.sqlobjecttree.mysql;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.BindIndexHolder;
import com.taobao.tddl.sqlobjecttree.Select;
import com.taobao.tddl.sqlobjecttree.WhereCondition;


public class MySelect extends Select {
	private RangeSelector limit=null;
	public RangeSelector getLimit() {
		return limit;
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

	public MySelect() {
		super();
		limit=new RangeSelector((MyWhereCondition)where);
	}
	public MySelect(BindIndexHolder holder){
		super(holder);
		limit=new RangeSelector((MyWhereCondition)where);
	}
	@Override
	protected WhereCondition getWhereCondition() {
		return new MyWhereCondition();
	}
	public void appendSQL(StringBuilder sb) {
		super.buildSelectString(sb);
		limit.appendSQL(sb);
	}
	
	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		//bugfix:原来的代码中嵌套的sql limit反向输出后在括号外
		sb = buildSqlStringWithModifiable(oraTabName, list, sb);
		sb = limit.regTableModifiable(oraTabName, list, sb);
		return sb;
	}
	
	//已经将Distinct 当做了一个方法， Distinct 和后面的col，一起作为一个col
	//fixed by mazhidan.pt
//	@Override
//	public List<String> getDistinctColumn() {
//		if(distinct==null){
//			return null;
//		}
//		
//		List<String> distinctColumns=new ArrayList<String>();
//	    for(Column co:columns.getColumnsList()){
//	    	distinctColumns.add(co.getColumn());
//	    }
//	    
//		return distinctColumns;
//	}
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		limit.appendSQL(sb);
		return sb.toString();
	}
	
	public boolean hasHavingCondition(){
		if(having.getExpGroup().getExpressions().size()!=0){
			return true;
		}else{
			return false;
		}
	}
}
