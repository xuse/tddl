package com.taobao.tddl.sqlobjecttree.common.expression;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.common.sqlobjecttree.Value;
import com.taobao.tddl.sqlobjecttree.Expression;
import com.taobao.tddl.sqlobjecttree.Function;
import com.taobao.tddl.sqlobjecttree.RowJepVisitor;
import com.taobao.tddl.sqlobjecttree.Utils;
import com.taobao.tddl.sqlobjecttree.common.ComparableElement;
import com.taobao.tddl.sqlobjecttree.common.value.UnknowValueObject;



/**
 * 表示可比较的Expression
 * 子类就是可比较的方法的具体类型 如 col = 1, col >1,col <1...等等。
 * 
 * @author shenxun
 *
 */
public abstract class ComparableExpression implements Expression{
	private boolean rownum;
	protected Object left;
	protected Object right;
	protected abstract int getComparativeOperation();
	public abstract String getRelationString();
	public void appendSQL(StringBuilder sb) {
		Utils.appendSQLList(left, sb);
		sb.append(getRelationString());
		Utils.appendSQLList(right, sb);
	}
	public void eval(RowJepVisitor visitor, boolean inAnd) {
		String colName=null;
		if (left instanceof Column) {
			colName = ((Column) left).getColumn();
		}else if(left instanceof Function){
			colName = ((Function)left).getNestedColName();
		}
		int operator=getComparativeOperation();
		if (right instanceof Value) {
			Value val = (Value) right;
			Comparable<?> temp =val.eval();
		
			if(colName==null){
				throw new IllegalArgumentException("sql元素："+left+"|"+getRelationString()+"|"+right+"中未找到指定的列名,目前只支持列名字段在表达式左边");
			}
			visitor.put(colName.toUpperCase(), new ComparableElement(
					temp, inAnd, operator));
		} else {
			//如果不是Value对象
			if(colName==null){
				if(right instanceof Column){
					throw new IllegalArgumentException("sql元素："+left+"|"+getRelationString()+"|"+right+"不支持列名写在Comparative右边");
				}
			}else{
			if (right instanceof Comparable) {
					visitor.put(colName.toUpperCase(), new ComparableElement(
							(Comparable<?>) right, inAnd, operator));
				} else if (right == null) {
					visitor.put(colName.toUpperCase(), new ComparableElement(
							(Comparable<?>) right, inAnd, operator));
				} else {
					visitor.put(colName.toUpperCase(), new ComparableElement(
							UnknowValueObject.getUnknowValueObject(), inAnd,
							operator));
				}
			}
		}
	}
	public Object getLeft() {
		return left;
	}
	public void setLeft(Object left) {
		this.left = left;
	}
	public Object getRight() {
		return right;
	}
	public void setRight(Object right) {
		this.right = right;
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		sb=Utils.appendSQLListWithList(oraTabName, left, sb, list);
		sb.append(getRelationString());
		sb=Utils.appendSQLListWithList(oraTabName, right, sb, list);
		return sb;
	}
//	 public void regTableModifiable(String oraTabName,List<ModifiableTableName> list){
//		 ModifiedTableUtils.regModifiedTab(left, oraTabName, list);
//		 ModifiedTableUtils.regModifiedTab(right, oraTabName, list);
//	 }
	public void setRownum(boolean rownum) {
		this.rownum = rownum;
	}
	public boolean isRownum() {
		return rownum;
	}

}
