package com.taobao.tddl.sqlobjecttree.common.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.SQLFragment;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.sqlobjecttree.Constant;
import com.taobao.tddl.sqlobjecttree.Expression;
import com.taobao.tddl.sqlobjecttree.RowJepVisitor;
import com.taobao.tddl.sqlobjecttree.Utils;
import com.taobao.tddl.sqlobjecttree.common.ComparableElement;




public  class ExpressionGroup implements Expression{


	protected List<Expression> expressions = new ArrayList<Expression>();  

	protected String getConjunction(){
		return " and ";
	}
	public void addExpression(Expression expression){
		expressions.add(expression);
	}
	
	public void addExpressionGroup(ExpressionGroup expGroup){
		expressions.add(expGroup);
	}
	

	

//	public Object getValue(String key) {
//		for (SQLFragment expression : expressions) {
//			if (expression instanceof TwoArgsExpression) {
//				TwoArgsExpression expr = (TwoArgsExpression) expression;
//				if (expr.getRelation() == TwoArgsExpression.RELATION.EQ) {
//					Object left = expr.getLeft();
//					Object right = expr.getRight();
//					if ((left instanceof ColumnObject) && !(right instanceof ColumnObject)) {
//						if (left.toString().equalsIgnoreCase(key)) {
//							return right;
//						}
//					}
//					if (!(left instanceof ColumnObject) && (right instanceof ColumnObject)) {
//						if (right.toString().equalsIgnoreCase(key)) {
//							return left;
//						}
//					}
//				}
//			}
//		}
//
//		return null;
//	}


	public void appendSQL(StringBuilder sb) {
		boolean appendSplitter = false;
		for (SQLFragment expression : expressions) {
			if (appendSplitter) {
				sb.append(getConjunction());
			} else {
				appendSplitter = true;
			}
			if(expression instanceof OrExpressionGroup){
				sb.append("(");
				Utils.appendSQL(expression, sb);
				sb.append(")");
			}else{
				Utils.appendSQL(expression, sb);
			}
			
		}
		
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean appendSplitter = false;
		for (SQLFragment expression : expressions) {
			if (appendSplitter) {
				sb.append(getConjunction());
			} else {
				appendSplitter = true;
			}
			if(this instanceof OrExpressionGroup) {
				if(Constant.useToString(expression)) {
					sb.append(expression.toString());
				} else {
					Utils.appendSQL(expression, sb);
				}
			} else if(!Constant.useToString(expression)) {
				Utils.appendSQL(expression, sb);
			} else if(expression instanceof ComparableExpression) {
				sb.append(expression.toString());
			} else if(expression instanceof ExpressionGroup) {
				if(expression instanceof OrExpressionGroup) {
					sb.append('(');
					sb.append(expression.toString());
					sb.append(')');
				} else {
					sb.append(expression.toString());
				}
			} else {
				sb.append(expression.toString());
			}
		}
		return sb.toString();
	}

	public void eval(RowJepVisitor visitor, boolean inAnd) {
		for(Expression e :expressions){
			if(e instanceof OrExpressionGroup){
				RowJepVisitor vis=new RowJepVisitor();
				e.eval(vis, true);
				Map<String, Comparative> mp= vis.getComparable();
				for(Entry<String,Comparative> et:mp.entrySet()){
					visitor.put(et.getKey(), new ComparableElement(et.getValue(),true,et.getValue().getComparison()));
				}
			}else{
				e.eval(visitor, true);
			}
		}
		
	}

	public List<Expression> getExpressions() {
		return expressions;
	}

//	public void regTableModifiable(String oraTabName,
//			List<ModifiableTableName> list) {
//		for(Expression exp:expressions){
//			exp.regTableModifiable(oraTabName, list);
//		}
//	}
	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		boolean appendSplitter = false;
		for (SQLFragment expression : expressions) {
			if (appendSplitter) {
				sb.append(getConjunction());
			} else {
				appendSplitter = true;
			}
			if(expression instanceof OrExpressionGroup){
				sb.append("(");
				sb=Utils.appendSQLWithList(oraTabName, expression, sb, list);
				sb.append(")");
			}else{
				sb=Utils.appendSQLWithList(oraTabName, expression, sb, list);
			}
			
		}
		return sb;
	}
}
