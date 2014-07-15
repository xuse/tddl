package com.taobao.tddl.sqlobjecttree.mysql;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.SQLFragment;
import com.taobao.tddl.sqlobjecttree.DMLCommon;
import com.taobao.tddl.sqlobjecttree.RangeWrapper;
import com.taobao.tddl.sqlobjecttree.SkipWrapper;
import com.taobao.tddl.sqlobjecttree.Utils;
import com.taobao.tddl.sqlobjecttree.common.value.BindVar;


public class RangeSelector implements SQLFragment {

	private MyWhereCondition where = null;

	public RangeSelector(MyWhereCondition where) {
		this.where = where;
	}


	public void appendSQL(StringBuilder sb) {
		Object start = where.getStart();
		Object range = where.getRange();
		if (range != null) {
			sb.append(" LIMIT ");
			if (start != null) {
				Utils.appendSQL(start, sb);
				sb.append(",");
			}
			if (range != null) {
				Utils.appendSQL(range, sb);
			}
		}
	}

	public int getSkip(List<Object> param) {
		Object st = where.getStart();
		if (st == null) {
			return DMLCommon.DEFAULT_SKIP_MAX;
		}
		if (st instanceof Integer) {
			return (Integer) st ;
		}
		if (st instanceof BindVar) {
			int index = ((BindVar) st).getIndex();
			Object obj = param.get(index);
			if (obj instanceof Long) {
				throw new IllegalArgumentException("row selecter can't handle long data");
			}else if(obj instanceof Integer){
				return ((Integer) obj);
			}else{
				throw new IllegalArgumentException("绑定变量发生错误:当前的绑定变量是" + obj+"不是一个int对象");
			}
			
		}
		return DMLCommon.DEFAULT_SKIP_MAX;
	}

	public int getRange(List<Object> param) {

		Object range = where.getRange();
		Integer intVal = null;
		if (range instanceof Integer) {
			intVal = (Integer) range;
		} else if (range instanceof BindVar) {
			int index = ((BindVar) range).getIndex();
			Object obj = param.get(index);
			if (obj instanceof Long) {
				throw new IllegalArgumentException("row selecter can't handle long data");
			}else if(obj instanceof Integer){
				intVal= (Integer) obj;
			}else{
				throw new IllegalArgumentException("绑定变量发生错误:当前的绑定变量是" + obj+"不是一个int对象");
			}
		} else {
			intVal =DMLCommon.DEFAULT_SKIP_MAX;
		}
		return intVal;
	}


	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		Object start = where.getStart();
		Object range = where.getRange();
		if (range != null) {
			sb.append(" LIMIT ");
			if (start != null) {
				list.add(sb.toString());
				list.add(new SkipWrapper(start));
				sb=new StringBuilder();
				sb.append(",");
			}
			if (range != null) {
				list.add(sb.toString());
				list.add(new RangeWrapper(range));
				sb=new StringBuilder();
			}
		}
		return sb;
	}
}
