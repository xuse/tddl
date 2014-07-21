package com.taobao.tddl.sqlobjecttree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.SQLFragment;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.sqlobjecttree.common.expression.ExpressionGroup;
import com.taobao.tddl.sqlobjecttree.common.expression.OrExpressionGroup;

/**
 * where语句的条件
 * 
 * @author shenxun
 * 
 */
public  class WhereCondition implements SQLFragment{
	private BindIndexHolder holder=null;

	public final static WhereCondition NULL_WHERE_CONDITION = new WhereCondition();
	private List<OrderByEle> orderByColumns = new ArrayList<OrderByEle>();
	private List<OrderByEle> groupByColumns = new ArrayList<OrderByEle>();
	private OrExpressionGroup expGroup = new OrExpressionGroup();



	public WhereCondition() {
	}
	
	public BindIndexHolder getHolder() {
		return holder;
	}

	public void setHolder(BindIndexHolder holder) {
		this.holder = holder;
	}

	public OrExpressionGroup getExpGroup() {
		return expGroup;
	}
	
	/**
	 * 清空whereCondition
	 */
	public void clear() {
		expGroup = null;

		if (null != groupByColumns) {
			groupByColumns.clear();
		}

		if (null != orderByColumns) {
			orderByColumns.clear();
		}
	}
	public int selfAddAndGet(){
		return holder.selfAddAndGet();
	}
	public void addAndExpression(ExpressionGroup exp) {
		expGroup.addExpressionGroup(exp);
	}


	public void appendSQL(StringBuilder sb) {

		StringBuilder temp=new StringBuilder();
		expGroup.appendSQL(temp);
		if(temp.length()!=0){
			sb.append(" WHERE ");
			sb.append(temp);
		}
		if (groupByColumns.size() != 0) {
			sb.append(" GROUP BY ");
			boolean comma = false;
			for (OrderByEle ol : groupByColumns) {
				if (comma) {
					sb.append(",");
				}
				comma = true;
				ol.appendSQL(sb);
			}
		}
		if (orderByColumns.size() != 0) {
			sb.append(" ORDER BY ");
			boolean comma = false;
			for (OrderByEle ol : orderByColumns) {
				if (comma) {
					sb.append(",");
				}
				comma = true;
				ol.appendSQL(sb);
			}
		}
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		StringBuilder temp=new StringBuilder();
		temp.append(expGroup.toString());
		if(temp.length()!=0){
			sb.append(" WHERE ");
			sb.append(temp);
		}
		if (groupByColumns.size() != 0) {
			sb.append(" GROUP BY ");

			boolean appendCommaSpliter = false;
			for (OrderByEle ol : groupByColumns) {
				if (appendCommaSpliter) {
					sb.append(",");
				}
				appendCommaSpliter = true;
				if (ol.getTable() != null) {
					sb.append(ol.getTable()).append(".");
				}

				sb.append(ol.getName());
				if (ol.isASC()) {
					sb.append(" ASC ");
				} else {
					sb.append(" DESC ");
				}
			}
		}
		if (orderByColumns.size() != 0) {
			sb.append(" ORDER BY ");

			boolean appendCommaSpliter = false;
			for (OrderByEle ol : orderByColumns) {
				if (appendCommaSpliter) {
					sb.append(",");
				}
				appendCommaSpliter = true;
				if (ol.getTable() != null) {
					sb.append(ol.getTable()).append(".");
				}

				sb.append(ol.getName());
				if (ol.isASC()) {
					sb.append(" ASC ");
				} else {
					sb.append(" DESC ");
				}
			}
		}
		return sb.toString();
	}
	public Map<String,Comparative> eval(){
		RowJepVisitor visitor=new RowJepVisitor();
		 expGroup.eval(visitor,false);
		 return visitor.getComparable();
	}


//	public void regTableModifiable(String oraTabName,
//			List<ModifiableTableName> list) {
//		expGroup.regTableModifiable(oraTabName, list);
//		for(OrderByEle ele:orderByColumns){
//			ele.regTableModifiable(oraTabName, list);
//		}
//	}
	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		
		if(expGroup.getExpressions().size()!=0){
			sb.append(" WHERE ");
			sb=expGroup.regTableModifiable(oraTabName, list, sb);
		}
		if (groupByColumns.size() != 0) {
			sb.append(" GROUP BY ");
			boolean comma = false;
			for (OrderByEle ol : groupByColumns) {
				if (comma) {
					sb.append(",");
				}
				comma = true;
				sb=ol.regTableModifiable(oraTabName, list, sb);
			}
		}
		if (orderByColumns.size() != 0) {
			sb.append(" ORDER BY ");
			boolean comma = false;
			for (OrderByEle ol : orderByColumns) {
				if (comma) {
					sb.append(",");
				}
				comma = true;
				sb=ol.regTableModifiable(oraTabName, list, sb);
			}
		}
		return sb;
	}
	
	public List<OrderByEle> getOrderByColumns() {
		return orderByColumns;
	}

	public void setOrderByColumns(List<OrderByEle> orderByColumns) {
		this.orderByColumns = orderByColumns;
	}

	public List<OrderByEle> getGroupByColumns() {
		return groupByColumns;
	}

	public void setGroupByColumns(List<OrderByEle> groupByColumns) {
		this.groupByColumns = groupByColumns;
	}

}
