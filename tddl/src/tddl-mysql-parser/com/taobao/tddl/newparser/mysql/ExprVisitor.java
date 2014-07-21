package com.taobao.tddl.newparser.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.parser.ast.expression.BinaryOperatorExpression;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.UnaryOperatorExpression;
import com.alibaba.cobar.parser.ast.expression.arithmeic.MinusExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.BetweenAndExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionGreaterThanExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionGreaterThanOrEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionIsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionLessOrGreaterThanExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionLessThanExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionLessThanOrEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionNotEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.InExpression;
import com.alibaba.cobar.parser.ast.expression.logical.LogicalAndExpression;
import com.alibaba.cobar.parser.ast.expression.logical.LogicalOrExpression;
import com.alibaba.cobar.parser.ast.expression.misc.InExpressionList;
import com.alibaba.cobar.parser.ast.expression.misc.QueryExpression;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.ParamMarker;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Avg;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Count;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Max;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Min;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Sum;
import com.alibaba.cobar.parser.ast.expression.primary.literal.IntervalPrimary;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralBoolean;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralNull;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.cobar.parser.ast.expression.string.LikeExpression;
import com.alibaba.cobar.parser.ast.fragment.tableref.InnerJoin;
import com.alibaba.cobar.parser.ast.fragment.tableref.NaturalJoin;
import com.alibaba.cobar.parser.ast.fragment.tableref.OuterJoin;
import com.alibaba.cobar.parser.ast.fragment.tableref.StraightJoin;
import com.alibaba.cobar.parser.ast.fragment.tableref.SubqueryFactor;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReference;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.alibaba.cobar.parser.visitor.MySQLOutputASTVisitor;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeAND;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.newparser.mysql.parserresult.AggregateFunction;
import com.taobao.tddl.newparser.mysql.parserresult.Column;
import com.taobao.tddl.newparser.mysql.parserresult.MySqlStandardParserResult;
import com.taobao.tddl.newparser.mysql.parserresult.Table;
import com.taobao.tddl.sqlobjecttree.GroupFunctionType;
import com.taobao.tddl.sqlobjecttree.InExpressionObject;
import com.taobao.tddl.sqlobjecttree.common.TableNameImp;
import com.taobao.tddl.sqlobjecttree.common.value.BindVar;

public class ExprVisitor extends EmptySQLASTVisitor {
	private static Log logger=LogFactory.getLog(ExprVisitor.class);
	private Object columnOrValue;
	private Table table;
	private Comparative com;
	private MySqlStandardParserResult result;

	ExprVisitor(MySqlStandardParserResult result) {
		this.result = result;
	}

	public Object getColumnOrValue() {
		return columnOrValue;
	}

	public Comparative getComparative() {
		return com;
	}

	public Table getTable() {
		return table;
	}

	@Override
	public void visit(BetweenAndExpression node) {
		Expression first = node.getFirst();
		Expression second = node.getSecond();
		Expression third = node.getThird();

		ExprVisitor v = new ExprVisitor(this.result);
		first.accept(v);
		Column col = (Column) (v.getColumnOrValue());

		ExprVisitor lv = new ExprVisitor(this.result);
		second.accept(lv);
		Object lval = lv.getColumnOrValue();
		Comparative left = convertComparative(lval,
				Comparative.GreaterThanOrEqual);

		ExprVisitor rv = new ExprVisitor(this.result);
		third.accept(rv);
		Object rval = rv.getColumnOrValue();
		Comparative right = convertComparative(rval,
				Comparative.LessThanOrEqual);

		ComparativeAND com = new ComparativeAND();
		com.addComparative(left);
		com.addComparative(right);
		this.result.addColumn(col.nameUpperCase, com);
	}

	@Override
	public void visit(ComparisionIsExpression node) {
		// do nothing
		node.clearReplaceExpr();
	}

	@Override
	public void visit(LikeExpression node) {
		ExprVisitor first = new ExprVisitor(this.result);
		node.getFirst().accept(first);
		Column leftColumn = (Column) first.getColumnOrValue();

		ExprVisitor second = new ExprVisitor(this.result);
		node.getSecond().accept(second);

		Comparative icom = convertComparative(second.getColumnOrValue(),
				node.isNot() ? Comparative.NotLike : Comparative.Like);
		this.result.addColumn(leftColumn.nameUpperCase, icom);

		// TODO:have third,but boolean filter not supported
		// just like: higherPreExpr 'NOT'? 'LIKE' higherPreExpr ('ESCAPE'
		// higherPreExpr)?
	}

	@Override
	public void visit(FunctionExpression node) {
		GroupFunctionType type = decideFunctionType(node);
		// 如果是我们关心的聚合函数才有意义去了解参数
		if (type != GroupFunctionType.NORMAL) {
			List<Object> aggres = new ArrayList<Object>();
			boolean argDistinct = isDistinct(node);
			List<Expression> expressions = node.getArguments();
			// 聚合函数参数只有一个，参数表达式类型没什么关系
			if (expressions != null && expressions.size() == 1) {
				ExprVisitor v = new ExprVisitor(this.result);
				expressions.get(0).accept(v);
				Object cv = v.getColumnOrValue();
				aggres.add(cv);
			} else {
				throw new RuntimeException(
						"tddl supported aggregate function not support more than one param!");
			}

			AggregateFunction func = new AggregateFunction(type, argDistinct,
					aggres, this.getSqlExprStr(node));
			result.setAggregate(func);
		} else {
			// 普通function需要解析一遍，获取参数column等信息
			List<Expression> expressions = node.getArguments();
			for (Expression expr : expressions) {
				ExprVisitor v = new ExprVisitor(this.result);
				expr.accept(v);
				// 普通function的信息不收集也行
			}
		}
	}

	private GroupFunctionType decideFunctionType(FunctionExpression node) {
		GroupFunctionType type = GroupFunctionType.NORMAL;
		if (node instanceof Min) {
			type = GroupFunctionType.MIN;
		} else if (node instanceof Max) {
			type = GroupFunctionType.MAX;
		} else if (node instanceof Count) {
			type = GroupFunctionType.COUNT;
		} else if (node instanceof Avg) {
			type = GroupFunctionType.AVG;
		} else if (node instanceof Sum) {
			type = GroupFunctionType.SUM;
		} else {
			type = GroupFunctionType.NORMAL;
		}

		return type;
	}

	/**
	 * get the whole expression string,include everything of this node
	 * 
	 * @param expr
	 * @return
	 */
	private String getSqlExprStr(Expression expr) {
		StringBuilder str = new StringBuilder();
		MySQLOutputASTVisitor oa = new MySQLOutputASTVisitor(str);
		expr.accept(oa);
		return str.toString();
	}

	/**
	 * get function arg is distinct
	 * 
	 * @param node
	 * @return
	 */
	private boolean isDistinct(FunctionExpression node) {
		if (node instanceof Avg) {
			return ((Avg) node).isDistinct();
		} else if (node instanceof Max) {
			return ((Max) node).isDistinct();
		} else if (node instanceof Min) {
			return ((Min) node).isDistinct();
		} else if (node instanceof Count) {
			return ((Count) node).isDistinct();
		} else if (node instanceof Sum) {
			return ((Sum) node).isDistinct();
			// if have other function support distinct,add here
		} else {
			return false;
		}
	}

	@Override
	public void visit(LiteralBoolean node) {
		this.columnOrValue = node.isTrue();
	}

	@Override
	public void visit(LiteralNull node) {
		this.columnOrValue = null;
	}

	@Override
	public void visit(LiteralNumber node) {
		this.columnOrValue = node.getNumber();
	}

	@Override
	public void visit(LiteralString node) {
		this.columnOrValue = node.getString();
	}

	@Override
	public void visit(Identifier node) {
		String owner = null;
		if (node.getParent() != null) {
			owner = node.getParent().getIdTextUpUnescape();
		}
		String columnName = node.getIdTextUpUnescape();
		// 先简单点，后面要进行column的多个表检测，包括子查询的分库分表字段不能和主查询不一致
		this.columnOrValue = new Column(columnName, owner, null);
	}

	@Override
	public void visit(IntervalPrimary node) {
		this.columnOrValue = this.getSqlExprStr(node);
	}

	@Override
	public void visit(ParamMarker node) {
		columnOrValue = node;
	}

	@Override
	public void visit(UnaryOperatorExpression node) {
		if (node instanceof MinusExpression) {
			columnOrValue = "-" + this.getSqlExprStr(node.getOperand());
		} else {
			// do nothing
		}
	}

	@Override
	public void visit(BinaryOperatorExpression node) {
		if (node instanceof ComparisionEqualsExpression) {
			this.handleBooleanFilter(node, Comparative.Equivalent);
		} else if (node instanceof ComparisionGreaterThanExpression) {
			this.handleBooleanFilter(node, Comparative.GreaterThan);
		} else if (node instanceof ComparisionGreaterThanOrEqualsExpression) {
			this.handleBooleanFilter(node, Comparative.GreaterThanOrEqual);
		} else if (node instanceof ComparisionLessOrGreaterThanExpression) {
			this.handleBooleanFilter(node, Comparative.NotEquivalent);
		} else if (node instanceof ComparisionLessThanExpression) {
			this.handleBooleanFilter(node, Comparative.LessThan);
		} else if (node instanceof ComparisionLessThanOrEqualsExpression) {
			this.handleBooleanFilter(node, Comparative.LessThanOrEqual);
		} else if (node instanceof ComparisionNotEqualsExpression) {
			this.handleBooleanFilter(node, Comparative.NotEquivalent);
		} else if (node instanceof InExpression) {
			this.handleInExpression((InExpression) node);
		} else {
			// do nothing,not care;
		}
	}

	@Override
	public void visit(TableRefFactor node) {
		String tableName = node.getTable().getIdTextUpUnescape();
		String schema = null;
		if (node.getTable().getParent() != null) {
			schema = node.getTable().getParent().getIdTextUpUnescape();
		}
		String alias = node.getAliasUnescapeUppercase();
		Table table = new Table(tableName, schema, alias);
		this.table = table;
		this.result.addTableName(getTableName(table));
	}

	private TableNameImp getTableName(Table table) {
		TableNameImp tbName = new TableNameImp();
		tbName.setTablename(table.nameUpperCase);
		tbName.setAlias(table.alias);
		tbName.setSchemaName(table.schema);
		return tbName;
	}

	@Override
	public void visit(InnerJoin node) {
		joinOn(node.getOnCond());

		TableReference ltable = node.getLeftTableRef();
		TableReference rtable = node.getRightTableRef();
		commonJoin(ltable, rtable);
	}

	@Override
	public void visit(NaturalJoin node) {
		TableReference ltable = node.getLeftTableRef();
		TableReference rtable = node.getRightTableRef();
		commonJoin(ltable, rtable);
	}

	@Override
	public void visit(OuterJoin node) {
		joinOn(node.getOnCond());
		TableReference ltable = node.getLeftTableRef();
		TableReference rtable = node.getRightTableRef();
		commonJoin(ltable, rtable);
	}

	@Override
	public void visit(StraightJoin node) {
		joinOn(node.getOnCond());
		TableReference ltable = node.getLeftTableRef();
		TableReference rtable = node.getRightTableRef();
		commonJoin(ltable, rtable);
	}

	private void commonJoin(TableReference ltable, TableReference rtable) {
		ExprVisitor lv = new ExprVisitor(this.result);
		ltable.accept(lv);

		ExprVisitor rv = new ExprVisitor(this.result);
		rtable.accept(rv);
	}

	private void joinOn(Expression cond) {
		if (cond != null && cond instanceof BinaryOperatorExpression) {
			if (!(cond instanceof ComparisionEqualsExpression)) {
				throw new RuntimeException(
						"not support join on not equal expression!");
			}

			ExprVisitor lv = new ExprVisitor(this.result);
			((BinaryOperatorExpression) cond).getLeftOprand().accept(lv);
			// Column lcol = (Column) lv.getColumnOrValue();

			ExprVisitor rv = new ExprVisitor(this.result);
			((BinaryOperatorExpression) cond).getRightOprand().accept(rv);
			// Column rcol = (Column) lv.getColumnOrValue();
		}
	}

	@Override
	public void visit(SubqueryFactor node) {
		SelectVisitor v = new SelectVisitor(this.result);
		node.accept(v);
		this.result.addSubQuery(v.getResult());
	}

	@Override
	public void visit(LogicalAndExpression node) {
		ComparativeAND and = new ComparativeAND();
		Map<String, Comparative> all = new HashMap<String, Comparative>();
		for (int i = 0; i < node.getArity(); i++) {
			Expression expr = node.getOperand(i);
			if (expr instanceof BinaryOperatorExpression) {
				Map<Column, Object> c = getComparative((BinaryOperatorExpression) expr);
				if (c == null) {
					// 有可能是in或者其他表达式
					continue;
				}
				for (Map.Entry<Column, Object> temp : c.entrySet()) {
					if(temp.getValue() instanceof Comparative){
						and.addComparative((Comparative)temp.getValue());
						Comparative com = all.get(temp.getKey().nameUpperCase);
						if (com != null) {
							if (com instanceof ComparativeAND) {
								((ComparativeAND) com).addComparative((Comparative)temp
										.getValue());
							} else {
								ComparativeAND a = new ComparativeAND();
								a.addComparative(com);
								a.addComparative((Comparative)temp.getValue());
								all.put(temp.getKey().nameUpperCase, a);
							}
						} else {
							all.put(temp.getKey().nameUpperCase, (Comparative)temp.getValue());
						}
					}else{
						//其他类型被忽略
						if (logger.isDebugEnabled()) {
							logger.debug("discard part of expr,param not suit,expr:"
									+ expr);
						}
					}
				}
			} else {
				ExprVisitor ev = new ExprVisitor(this.result);
				expr.accept(ev);
				and.addComparative(ev.getComparative());
			}
		}

		this.com = and;

		for (Map.Entry<String, Comparative> entry : all.entrySet()) {
			this.result.addColumn(entry.getKey(), entry.getValue());
		}
	}

	private Map<Column, Object> getBooleanFilter(
			BinaryOperatorExpression node, int relation) {
		Column key=null;
		Object value=null;
		
		if (node.getLeftOprand() instanceof DMLSelectStatement) {
			SelectVisitor lv = new SelectVisitor(this.result);
			node.getLeftOprand().accept(lv);
			// add to parserResult;
			this.result.addSubQuery(lv.getResult());
			value = lv.getResult();
		} else {
			ExprVisitor lv = new ExprVisitor(this.result);
			node.getLeftOprand().accept(lv);
			Object obj = lv.getColumnOrValue();
			if (obj instanceof Column) {
				key = (Column) obj;
			} else {
				value = convertComparative(obj, relation);
			}
		}

		if (node.getRightOprand() instanceof DMLSelectStatement) {
			SelectVisitor rv = new SelectVisitor(this.result);
			node.getRightOprand().accept(rv);
			// add to parserResult;
			this.result.addSubQuery(rv.getResult());
			value = rv.getResult();
		} else {
			ExprVisitor rv = new ExprVisitor(this.result);
			node.getRightOprand().accept(rv);
			Object obj = rv.getColumnOrValue();
			if (obj instanceof Column) {
				if (key == null) {
					key = (Column) obj;
				} else {
					value = obj;
				}
			} else {
				value = convertComparative(obj, relation);
			}
		}

		if (key != null && value != null) {
			Map<Column, Object> re = new HashMap<Column, Object>();
			re.put(key, value);
			if (value instanceof Comparative) {
				this.com = (Comparative) value;
			}
			return re;
		} else {
			return null;
		}
	}

	@SuppressWarnings("rawtypes")
	public static Comparative convertComparative(Object obj, int relation) {
		Comparative icom = null;
		if (obj instanceof ParamMarker) {
			icom = new Comparative(relation, new BindVar(
					((ParamMarker) obj).getParamIndex() - 1));
		} else if (obj instanceof Comparable || obj == null) {
			icom = new Comparative(relation, (Comparable) obj);
		} else {
			throw new IllegalArgumentException(
					"only support Comparable or BindVar(may be the right oprand in where condition )!illegal type is "
							+ obj.getClass().getName());
		}
		return icom;
	}

	public Map<Column, Object> getComparative(BinaryOperatorExpression node) {
		Map<Column, Object> com = null;
		if (node instanceof ComparisionEqualsExpression) {
			com = getBooleanFilter(node, Comparative.Equivalent);
		} else if (node instanceof ComparisionGreaterThanExpression) {
			com = getBooleanFilter(node, Comparative.GreaterThan);
		} else if (node instanceof ComparisionGreaterThanOrEqualsExpression) {
			com = getBooleanFilter(node, Comparative.GreaterThanOrEqual);
		} else if (node instanceof ComparisionLessOrGreaterThanExpression) {
			com = getBooleanFilter(node, Comparative.NotEquivalent);
		} else if (node instanceof ComparisionLessThanExpression) {
			com = getBooleanFilter(node, Comparative.LessThan);
		} else if (node instanceof ComparisionLessThanOrEqualsExpression) {
			com = getBooleanFilter(node, Comparative.LessThanOrEqual);
		} else if (node instanceof ComparisionNotEqualsExpression) {
			com = getBooleanFilter(node, Comparative.NotEquivalent);
		} else if (node instanceof InExpression) {
			this.handleInExpression((InExpression) node);
		} else {
			// do nothing,not care;
		}

		return com;
	}

	@Override
	public void visit(LogicalOrExpression node) {
		ComparativeOR or = new ComparativeOR();
		Map<String, Comparative> all = new HashMap<String, Comparative>();
		for (int i = 0; i < node.getArity(); i++) {
			Expression expr = node.getOperand(i);
			if (expr instanceof BinaryOperatorExpression) {
				Map<Column, Object> c = getComparative((BinaryOperatorExpression) expr);
				if (c == null) {
					// 有可能是in或者其他表达式
					continue;
				}
				for (Map.Entry<Column, Object> temp : c.entrySet()) {
					if(temp.getValue() instanceof Comparative){
						or.addComparative((Comparative)temp.getValue());
						Comparative com = all.get(temp.getKey().nameUpperCase);
						if (com != null) {
							if (com instanceof ComparativeOR) {
								((ComparativeOR) com).addComparative((Comparative)temp
										.getValue());
							} else {
								ComparativeOR a = new ComparativeOR();
								a.addComparative(com);
								a.addComparative((Comparative)temp.getValue());
								all.put(temp.getKey().nameUpperCase, a);
							}
						} else {
							all.put(temp.getKey().nameUpperCase,(Comparative) temp.getValue());
						}
					} else {
						// 其他类型被忽略
						if (logger.isDebugEnabled()) {
							logger.debug("discard part of expr,param not suit,expr:"
									+ expr);
						}
					}
				}
			} else {
				ExprVisitor ev = new ExprVisitor(this.result);
				expr.accept(ev);
				or.addComparative(ev.getComparative());
			}
		}
		this.com = or;

		for (Map.Entry<String, Comparative> entry : all.entrySet()) {
			this.result.addColumn(entry.getKey(), entry.getValue());
		}
	}

	private void handleBooleanFilter(BinaryOperatorExpression node, int relation) {
		Column key=null;
		Object value=null;
		
		if (node.getLeftOprand() instanceof DMLSelectStatement) {
			SelectVisitor lv = new SelectVisitor(this.result);
			node.getLeftOprand().accept(lv);
			// add to parserResult;
			this.result.addSubQuery(lv.getResult());
			value = lv.getResult();
		} else {
			ExprVisitor lv = new ExprVisitor(this.result);
			node.getLeftOprand().accept(lv);
			Object obj = lv.getColumnOrValue();
			if (obj instanceof Column) {
				key = (Column) obj;
			} else {
				value = convertComparative(obj, relation);
			}
		}

		if (node.getRightOprand() instanceof DMLSelectStatement) {
			SelectVisitor rv = new SelectVisitor(this.result);
			node.getRightOprand().accept(rv);
			// add to parserResult;
			this.result.addSubQuery(rv.getResult());
			value = rv.getResult();
		} else {
			ExprVisitor rv = new ExprVisitor(this.result);
			node.getRightOprand().accept(rv);
			Object obj = rv.getColumnOrValue();
			if (obj instanceof Column ) {
				if (key == null) {
					key = (Column) obj;
				} else {
					value = obj;
				}
			} else {
				value = convertComparative(obj, relation);
			}
		}

		if (key != null && value!=null && value instanceof Comparative) {
			this.com = (Comparative)value;
			this.result.addColumn(key.nameUpperCase, (Comparative)value);
		}
	}

	@SuppressWarnings("rawtypes")
	public void handleInExpression(InExpression node) {
		ExprVisitor left = new ExprVisitor(this.result);
		node.getLeftOprand().accept(left);
		Column leftColumn = (Column) left.getColumnOrValue();
		//如果leftColumn是一个表达式，其实就别去关注这个in 表达式了
		if (leftColumn != null) {
			Expression ex = node.getRightOprand();
			if (node.isNot()) {
				throw new IllegalArgumentException(
						"not suport 'not in' expression!");
			}

			if (ex instanceof InExpressionList) {
				List<Expression> elist = ((InExpressionList) ex).getList();
				List<Integer> bindVarIndexs = new ArrayList<Integer>();
				List<Object> bindVarValues = new ArrayList<Object>();

				ComparativeOR or = new ComparativeOR();
				for (Expression expr : elist) {
					ExprVisitor v = new ExprVisitor(this.result);
					expr.accept(v);
					Object obj = v.getColumnOrValue();
					if (obj instanceof ParamMarker) {
						int index = ((ParamMarker) (obj)).getParamIndex() - 1;
						or.addComparative(new Comparative(
								Comparative.Equivalent, new BindVar(index)));
						bindVarIndexs.add(index);
					} else {
						or.addComparative(new Comparative(
								Comparative.Equivalent, (Comparable) obj));
						bindVarValues.add(obj);
					}
				}

				InExpressionObject obj = new InExpressionObject(
						leftColumn.nameUpperCase, leftColumn.alias,
						bindVarIndexs, bindVarValues, this.getSqlExprStr(node));
				this.result.addInExprObj(obj);
				this.com = or;

				this.result.addColumn(leftColumn.nameUpperCase, or);
			} else if (ex instanceof QueryExpression) {
				throw new RuntimeException(
						"not support inExpression with subquery");
			}
		}
	}
}
