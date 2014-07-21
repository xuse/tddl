package com.taobao.tddl.sqlobjecttree.oracle;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.PageWrapper;
import com.taobao.tddl.sqlobjecttree.Utils;
import com.taobao.tddl.sqlobjecttree.common.expression.ComparableExpression;
import com.taobao.tddl.sqlobjecttree.common.value.BindVar;

/*
 * @author guangxia
 * @since 1.0, 2009-9-2 下午11:28:14
 * @author shenxun
 * 以前写的烂，重构一下
 */
public abstract class OraclePageWrapper implements PageWrapper{
	
	protected ComparableExpression comparableExpression;
	
	protected Integer index = null;
	
	protected Number value;
	
	public Integer getIndex() {
		return index;
	}
	public Number getValue() {
		return value;
	}
	public boolean canBeChange() {
		return true;
	}
	
	public OraclePageWrapper(ComparableExpression comp) {
		this.comparableExpression = comp;
		if (comparableExpression.getRight() instanceof BindVar) {
			 index = ((BindVar)comparableExpression.getRight()).getIndex();;
		} else {
			 Object right = comparableExpression.getRight();
			if(right instanceof Number){
				value = (Number) comparableExpression.getRight();	
			}else{
				throw new IllegalArgumentException("not number ,can't change " +
						""+comparableExpression.getRight());
			}
			
		}
	}
	
	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		sb = Utils.appendSQLListWithList(oraTabName, comparableExpression.getLeft(), sb, list);
		sb.append(comparableExpression.getRelationString());
		list.add(sb.toString());
		list.add(this);
		return new StringBuilder();
	}
	
}
