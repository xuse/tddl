package com.taobao.tddl.newparser.mysql;

import java.util.List;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.fragment.GroupBy;
import com.alibaba.cobar.parser.ast.fragment.Limit;
import com.alibaba.cobar.parser.ast.fragment.OrderBy;
import com.alibaba.cobar.parser.ast.fragment.SortOrder;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReference;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement.SelectDuplicationStrategy;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement.SelectOption;
import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.newparser.mysql.parserresult.Column;
import com.taobao.tddl.newparser.mysql.parserresult.MySqlStandardParserResult;
import com.taobao.tddl.sqlobjecttree.OrderByEle;

public class SelectVisitor extends EmptySQLASTVisitor {
	private MySqlStandardParserResult result;

	SelectVisitor(MySqlStandardParserResult result) {
		this.result = result;
	}

	@Override
	public void visit(DMLSelectStatement node) {
		TableReferences tables = node.getTables();
		if (tables != null) {
			handleFrom(tables);
		}

		List<Pair<Expression, String>> items = node.getSelectExprList();
		if (items != null) {
			handleSelectItems(node);
		}

		// not support having
		Expression havingExpr = node.getHaving();
		if(havingExpr!=null){
			handleHavingCondition(havingExpr);
		}

		Expression whereExpr = node.getWhere();
		if (whereExpr != null) {
			handleWhereCondition(whereExpr);
		}

		OrderBy orderBy = node.getOrder();
		if (orderBy != null) {
			handleOrderBy(orderBy);
		}

		GroupBy groupBy = node.getGroup();
		if (groupBy != null) {
			handleGroupBy(groupBy);
		}

		Limit limit = node.getLimit();
		if (limit != null) {
			handleLimit(limit);
		}
	}

	private void handleSelectItems(DMLSelectStatement node) {
		List<Pair<Expression, String>> items = node.getSelectExprList();
		SelectOption option = node.getOption();
		for (Pair<Expression, String> item : items) {
			Expression expr = item.getKey();
			ExprVisitor ev = new ExprVisitor(this.result);
			expr.accept(ev);
			if (option.resultDup == SelectDuplicationStrategy.DISTINCT
					&& ev.getColumnOrValue() instanceof Column) {
				this.result
						.addDistinctCol(((Column) ev.getColumnOrValue()).nameUpperCase);
			}
		}
	}

	private void handleFrom(TableReferences tables) {
		List<TableReference> trs = tables.getTableReferenceList();
		for (TableReference tr : trs) {
			ExprVisitor mtv = new ExprVisitor(this.result);
			tr.accept(mtv);
		}
	}

	private void handleWhereCondition(Expression whereExpr) {
		ExprVisitor mev = new ExprVisitor(this.result);
		whereExpr.accept(mev);
	}

	private void handleHavingCondition(Expression havingExpr){
		//having 不需要处理,设置下标记就ok
//		MySqlExprVisitor mev = new MySqlExprVisitor(this.result);
//		havingExpr.accept(mev);
		this.result.setHasHaving(true);
	}
	
	private void handleOrderBy(OrderBy orderBy) {
		List<Pair<Expression, SortOrder>> olist = orderBy.getOrderByList();

		for (Pair<Expression, SortOrder> p : olist) {
			Expression expr = p.getKey();
			ExprVisitor v = new ExprVisitor(this.result);
			expr.accept(v);
			Column c = (Column) v.getColumnOrValue();
			OrderByEle ele = new OrderByEle(c.owner, c.nameUpperCase,
					p.getValue() == SortOrder.ASC ? true : false);
			this.result.addOrderBy(ele);
		}
	}

	private void handleGroupBy(GroupBy groupBy) {
		List<Pair<Expression, SortOrder>> olist = groupBy.getOrderByList();

		for (Pair<Expression, SortOrder> p : olist) {
			Expression expr = p.getKey();
			ExprVisitor v = new ExprVisitor(this.result);
			expr.accept(v);
			Column c = (Column) v.getColumnOrValue();
			OrderByEle ele = new OrderByEle(c.owner, c.nameUpperCase,
					p.getValue() == SortOrder.ASC ? true : false);
			this.result.addGroupBy(ele);
		}
	}

	private void handleLimit(Limit limit) {
		this.result.setLimit(limit);
	}

	public MySqlStandardParserResult getResult() {
		return result;
	}

	public void setResult(MySqlStandardParserResult result) {
		this.result = result;
	}
}
