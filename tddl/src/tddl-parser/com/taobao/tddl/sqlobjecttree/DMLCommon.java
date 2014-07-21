package com.taobao.tddl.sqlobjecttree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.common.sqlobjecttree.SQLFragment;
import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeBaseList;
import com.taobao.tddl.sqlobjecttree.common.TableNameSubQueryImp;
import com.taobao.tddl.sqlobjecttree.common.expression.ComparableExpression;
import com.taobao.tddl.sqlobjecttree.common.expression.ExpressionGroup;
import com.taobao.tddl.sqlobjecttree.common.expression.InExpression;
import com.taobao.tddl.sqlobjecttree.common.expression.NotInExpression;
import com.taobao.tddl.sqlobjecttree.common.value.BindVar;
import com.taobao.tddl.sqlobjecttree.common.value.UnknowValueObject;
import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.HandlerContainer;
import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.PlaceHolderReplaceHandler;
import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.RangePlaceHandler;
import com.taobao.tddl.sqlobjecttree.traversalAction.GroupByTraversalAction;
import com.taobao.tddl.sqlobjecttree.traversalAction.OrderByTraversalAction;
import com.taobao.tddl.sqlobjecttree.traversalAction.TableNameTraversalAction;
import com.taobao.tddl.sqlobjecttree.traversalAction.TraversalSQLAction;
import com.taobao.tddl.sqlobjecttree.traversalAction.TraversalSQLEvent;
import com.taobao.tddl.sqlobjecttree.traversalAction.TraversalSQLEvent.StatementType;

/**
 * insert update delete select公共基类
 * 
 * @author shenxun
 * 
 */
public abstract class DMLCommon implements Statement, SqlParserResult,
		ComparativeMapChoicer {
	protected BindIndexHolder holder = new BindIndexHolder();

	protected List<OrderByEle> orderByEle = Collections.emptyList();
	protected List<OrderByEle> groupByEle = Collections.emptyList();
	protected List<InExpressionObject> inObjList = new ArrayList<InExpressionObject>(
			1);

	protected List<Hint> hints = new ArrayList<Hint>(1);

	public DMLCommon(BindIndexHolder holder) {
		this.holder = holder;
	}

	public DMLCommon() {
	}

	public BindIndexHolder getIndexHolder() {
		return holder;
	}

	/**
	 * 用于映射别名与实际sql中的元素之间的映射关系，这样可以直接通过别名找到对应的元素
	 * 
	 * 目前暂时只支持别名到表对象,与查询列名对象的映射关系。
	 * 
	 * 主要用于解决嵌套查询中taobao内定的str2varlist和str2numlist函数。
	 */
	protected volatile Map<String, SQLFragment> aliasToSQLFragementMap = new HashMap<String, SQLFragment>();

	/**
	 * 这是一个多层where条件结构，如果sql是嵌套的，那么这个List会与嵌套sql中的每一层一一对应
	 * 
	 * 。0层对应sql最外层，依次往下。
	 * 
	 * 每一层都是该sql层中where条件的一个结果合并后的Comparative对象的Map.
	 * 
	 * 使用的时候，会遍历整个List,找到分库和分表的column,如果有多个，则会抛出异常。
	 * 
	 */
	protected volatile List<Map<String, Comparative>> repListMap = new ArrayList<Map<String, Comparative>>();
	/**
	 * 包含String与limit对象，核心的原理是分离一条sql中需要变化的和不需要变化的对象，把所有需要添加表名的地方空出来
	 * 只保留sql除了需要替换的表名以外的其他字段，同时在一次对树的分析中，还会分离limit中的数据对象。 相当于一个缓存了sql中不变元素的缓存。
	 */
	protected final List<Object> modifiableList = new ArrayList<Object>(2);
	/**
	 * 如果没有skip和max会返回此值
	 */
	public final static int DEFAULT_SKIP_MAX = -1000;
	Set<String> tableName = null;

	/**
	 * 表名List
	 */
	protected List<TableName> tbNames = new ArrayList<TableName>(2);

	/**
	 * 从多层where条件中根据partnationSet选择符合要求的列，将绑定变量赋值，并且返回列和他对应的值。
	 * 
	 * 如果多层条件中的不同层都出现了同一个列，则抛异常出去。
	 * 
	 * 获取ComparativeMap. map的key 是列名 value是绑定变量后的{@link Comparative}
	 * 如果是个不可赋值的变量，则不会返回。 不可赋值指的是，虽然可以解析，但解析以后的结果不能进行计算。 如where col =
	 * concat(str,str); 这种SQL虽然可以解析，但因为对应的处理函数没有完成，所以是不能赋值的。这种情况下col
	 * 是不会被放到返回的map中的。
	 * 
	 * @param arguments
	 * @param partnationSet
	 * @param copiedMap
	 * @return
	 */
	public final Map<String, Comparative> getColumnsMap(List<Object> arguments,
			Set<String> partnationSet) {
		Map<String, Comparative> copiedMap = new HashMap<String, Comparative>(
				partnationSet.size());
		for (String aArgument : partnationSet) {
			/*
			 * for (Map<String, Comparative> map : repListMap) { //modified by
			 * shenxun. 因为新规则引擎现在传入的str有可能是大小写敏感的。
			 * //但在sql这一层实际上却大小不敏感了。因此要显示的转换一下 Comparative temp =
			 * map.get(aArgument.toUpperCase()); if (temp != null) { if
			 * (copiedMap.containsKey(aArgument)) { throw new
			 * IllegalArgumentException(
			 * "不允许在多层sql的where条件中出现多个出现分库字段的点。有问题的分库字段是：" + aArgument); }
			 * Comparative comparative = temp.getVal(arguments,
			 * aliasToSQLFragementMap); if
			 * (!containsUnknowValueObject(comparative)) {
			 * //但放入map的还必须是原始的大小写敏感的字串。否则规则那边无法使用 copiedMap.put(aArgument,
			 * comparative); } } }
			 */
			Comparative comparative = getColumnComparative(arguments, aArgument);
			if (comparative != null) {
				copiedMap.put(aArgument, comparative);
			}
		}
		return copiedMap;
	}

	public Comparative getColumnComparative(List<Object> arguments,
			final String aArgument) {
		Comparative res = null;
		String upperCaseArg = aArgument.toUpperCase();
		for (Map<String, Comparative> map : repListMap) {
			// modified by shenxun. 因为新规则引擎现在传入的str有可能是大小写敏感的。
			// 但在sql这一层实际上却大小不敏感了。因此要显示的转换一下
			Comparative temp = map.get(upperCaseArg);
			if (temp != null) {
				// if (copiedMap.containsKey(aArgument)) {
				if (res != null) {
					throw new IllegalArgumentException(
							"不允许在多层sql的where条件中出现多个出现分库字段的点。有问题的分库字段是："
									+ aArgument);
				}
				Comparative comparative = temp.getVal(arguments,
						aliasToSQLFragementMap);
				if (!containsUnknowValueObject(comparative)) {
					// 但放入map的还必须是原始的大小写敏感的字串。否则规则那边无法使用
					// copiedMap.put(aArgument, comparative);
					res = comparative;
				}
			}
		}
		return res;
	}

	protected boolean containsUnknowValueObject(Comparative comparative) {
		;
		if (comparative.getValue() instanceof UnknowValueObject) {
			return true;
		} else if (comparative instanceof ComparativeBaseList) {
			List<Comparative> list = ((ComparativeBaseList) comparative)
					.getList();
			if (list != null) {
				// 如果内部循环有一个包含unknowValueObject的就直接返回true;
				for (Comparative c : list) {
					if (containsUnknowValueObject(c)) {
						return true;
					}
				}
				// 一个都没有的情况下，返回false;
				return false;
			}
		}
		return false;
	}

	public List<TableName> getTbNames() {
		return tbNames;
	}

	public void addTable(TableName tableName) {
		this.tbNames.add(tableName);
	}

	/**
	 * 这个方法应该确保只在方法内被调用。非线程安全
	 */
	public void init() {

		initAliasAndComparableMap(aliasToSQLFragementMap, repListMap);

		registerTraversalActionAndGet();

		registerUnmodifiableSqlOutputFragement();
	}

	/**
	 * 注册遍历每一个where条件的action,并且获取结果
	 */
	public void registerTraversalActionAndGet() {
		// 添加当前一行sql的wheree条件数据到List
		List<TraversalSQLAction> traversalSQLActions = new ArrayList<TraversalSQLAction>();

		TableNameTraversalAction tbNameaction = new TableNameTraversalAction();

		traversalSQLActions.add(tbNameaction);

		OrderByTraversalAction orderby = new OrderByTraversalAction();
		traversalSQLActions.add(orderby);

		GroupByTraversalAction groupby = new GroupByTraversalAction();
		traversalSQLActions.add(groupby);

		traversalSQL(traversalSQLActions, null);

		tableName = tbNameaction.getTableName();

		orderByEle = orderby.getOrderByEles();
		groupByEle = groupby.getGroupByEles();
	}

	/**
	 * 注册可变sql中不变的sql
	 */
	protected void registerUnmodifiableSqlOutputFragement() {

		StringBuilder sb = new StringBuilder();

		sb = regTableModifiable(tableName, modifiableList, sb);

		modifiableList.add(sb.toString());

	}

	public List<OrderByEle> getOrderByEles() {
		return orderByEle;
	}

	public List<OrderByEle> getGroupByEles() {
		return groupByEle;
	}

	/**
	 * 存放了表的别名和select |column| from 中column的别名
	 * 
	 * @param sqlAliasMap
	 */
	public void buildAliasToTableAndColumnMapping(
			Map<String, SQLFragment> sqlAliasMap) {

		for (TableName name : tbNames) {

			name.appendAliasToSQLMap(sqlAliasMap);

		}
	}

	/**
	 * 初始化别名的map和ComparativeMap
	 * 
	 * @param sqlAliasMap
	 *            因为只需要一个sqlAliasMap,因此用最外层的sqlAliasMap
	 * @param repListMap
	 *            因为只需要一个ComparableMap,所以传入最外层的repListMap
	 */
	protected void initAliasAndComparableMap(
			Map<String, SQLFragment> aliasToSQLFragementMap,
			List<Map<String, Comparative>> repListMap) {
		buildAliasToTableAndColumnMapping(aliasToSQLFragementMap);
		boolean hasOneSubSelect = false;
		// tbNames应该取当前嵌套中的tbNames
		for (TableName name : tbNames) {
			if (name instanceof TableNameSubQueryImp) {

				if (hasOneSubSelect) {
					throw new IllegalArgumentException("同级sql不允许出现多个子sql");
				}
				hasOneSubSelect = true;
				TableNameSubQueryImp subSql = (TableNameSubQueryImp) name;
				Select select = subSql.getSubSelect();
				select.initAliasAndComparableMap(aliasToSQLFragementMap,
						repListMap);
			}

		}
		repListMap.add(getSubColumnsMap());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.taobao.tddl.common.sqlobjecttree.SQLFragment#regTableModifiable(java
	 * .lang.String, java.util.List, java.lang.StringBuilder)
	 */
	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		boolean hasMoreElement = false;
		for (TableName tbName : tbNames) {
			if (hasMoreElement) {
				sb.append(",");
			} else {
				hasMoreElement = true;
			}
			sb = tbName.regTableModifiable(oraTabName, list, sb);
		}
		sb.append(" ");
		return sb;
	}

	public boolean hasTable() {
		return tbNames.size() == 0 ? false : true;
	}

	/**
	 * 如果一个子类有where条件，那么就能够获取这个子类where条件中所有 列->值的map
	 * 
	 * @return
	 */
	protected abstract Map<String, Comparative> getSubColumnsMap();

	/**
	 * 遍历每一个sql的表名部分，优先查询当前的where条件，然后是表名，如果表名内有嵌套 也按照上述逻辑进行循环遍历
	 * 
	 * @param traversalSQLActions
	 */
	public void traversalSQL(List<TraversalSQLAction> traversalSQLActions,
			StatementType type) {
		if (type != null) {
			notifyAll(traversalSQLActions, this, type);
		} else {
			notifyAll(traversalSQLActions, this, StatementType.NORMAL);
		}
		WhereCondition where = getSubWhereCondition();
		if (where != null) {
			ExpressionGroup expgrp = where.getExpGroup();
			traversalExpressionGroup(expgrp, traversalSQLActions);
		}
		// 遍历每一个SQL，嵌套内部sql也进行遍历
		for (TableName tbName : tbNames) {
			if (tbName instanceof TableNameSubQueryImp) {
				Select select = ((TableNameSubQueryImp) tbName).getSubSelect();
				select.traversalSQL(traversalSQLActions, StatementType.TABLE);
			}
		}
	}

	/**
	 * 遍历整个ExpressionGroup,查找里面的Select
	 * TODO:这段代码有严重的问题,让通过action链处理,却不传递类型,如何让action识别?
	 * 
	 * @param expgrp
	 * @param travelsalSQLActions
	 */
	protected void traversalExpressionGroup(ExpressionGroup expgrp,
			List<TraversalSQLAction> traversalSQLActions) {
		List<Expression> exps = expgrp.getExpressions();
		for (Expression exp : exps) {
			if (exp instanceof ExpressionGroup) {
				// 表达式组嵌套。
				traversalExpressionGroup((ExpressionGroup) exp,
						traversalSQLActions);
			} else if (exp instanceof ComparableExpression) {
				Object obj = ((ComparableExpression) exp).getRight();
				whereArgumentHandler(traversalSQLActions, obj);
				obj = ((ComparableExpression) exp).getLeft();
				whereArgumentHandler(traversalSQLActions, obj);
			} else if (exp instanceof InExpression) {
				Object obj = ((InExpression) exp).getRight();
				whereArgumentHandler(traversalSQLActions, obj);
				obj = ((InExpression) exp).getLeft();
				whereArgumentHandler(traversalSQLActions, obj);
				inExpressionHandle((InExpression) exp);
			} else if (exp instanceof NotInExpression) {
				Object obj = ((NotInExpression) exp).getRight();
				whereArgumentHandler(traversalSQLActions, obj);
				obj = ((NotInExpression) exp).getLeft();
				whereArgumentHandler(traversalSQLActions, obj);
			} else {
				throw new IllegalStateException("should not be here");
			}
		}
	}

	/**
	 * id in分组必须知道id in信息 (需要重构)
	 * 
	 * @param right
	 * @param left
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void inExpressionHandle(InExpression exp) {
		Column column = (Column) exp.getLeft();
		Object values = exp.getRight();

		if (values instanceof List) {
			StringBuilder expStr = new StringBuilder();
			exp.appendSQL(expStr);

			List valuesList = (List) values;
			if (valuesList.get(0) instanceof BindVar) {
				// 还是有可能有 id in(?,?,1,2),这种情况,就不支持了
				for (Object obj : valuesList) {
					if (!(obj instanceof BindVar)) {
						return;
					}
				}
				List<Integer> indexs = new ArrayList<Integer>(valuesList.size());
				List<BindVar> bvs = (List<BindVar>) valuesList;
				for (BindVar bv : bvs) {
					indexs.add(bv.getIndex());
				}
				this.inObjList.add(new InExpressionObject(column.getColumn(),
						column.getAlias(), indexs, null, expStr.toString()));
			} else {
				// 还是有可能有 id in(?,?,1,2),这种情况,就不支持了
				for (Object obj : valuesList) {
					if (obj instanceof BindVar) {
						return;
					}
				}

				List<Object> indexs = new ArrayList<Object>(valuesList.size());
				List<Object> bvs = (List<Object>) valuesList;
				for (Object bv : bvs) {
					indexs.add(bv);
				}
				this.inObjList.add(new InExpressionObject(column.getColumn(),
						column.getAlias(), null, indexs, expStr.toString()));
			}
		} else if (values instanceof Select) {
			// 暂时不支持id in 在子查询中的归组,按原来方案走
		}
	}

	private static void whereArgumentHandler(
			List<TraversalSQLAction> traversalSQLActions, Object obj) {
		if (obj instanceof Select) {
			notifyAll(traversalSQLActions, (Select) obj, StatementType.WHERE);
		}
	}

	private static void notifyAll(List<TraversalSQLAction> travelsarSQLActions,
			DMLCommon dmlc, StatementType type) {
		for (TraversalSQLAction action : travelsarSQLActions) {
			action.actionProformed(new TraversalSQLEvent(StatementType.TABLE,
					dmlc));
		}
	}

	/**
	 * 获取where条件，如果有的话，米有会返回null
	 */
	public abstract WhereCondition getSubWhereCondition();

	@SuppressWarnings("unchecked")
	public Set<String> getTableName() {
		return (tableName == null ? Collections.EMPTY_SET : tableName);
	}

	/**
	 * 遍历整个树，获取limit m,n中的n 或oracle rownum < ?中的?
	 * 
	 * @param param
	 * @return
	 */
	protected int getRangeOrMax(List<Object> param) {
		int max = DEFAULT_SKIP_MAX;
		int temp = DEFAULT_SKIP_MAX;

		for (TableName tbName : tbNames) {
			// 查看SQL的嵌套SQL,里面是否有RangeOrMax值。如果多个嵌套SQL组成的多层嵌套SQL中，有
			// 多个层级都有rangeOrMax,则挑选最大的。
			if (tbName instanceof TableNameSubQueryImp) {
				temp = ((TableNameSubQueryImp) tbName).getSubSelect()
						.getRangeOrMax(param);
				// 无论怎么优化 max值,值都肯定是最大的有意义，
				if (temp > max) {
					max = temp;
				}
			}
		}
		return max;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.taobao.tddl.sqlobjecttree.SqlParserResult#getSkip(java.util.List)
	 */
	public int getSkip(List<Object> param) {
		int skip = DEFAULT_SKIP_MAX;
		int temp = DEFAULT_SKIP_MAX;
		for (TableName tbName : tbNames) {
			if (tbName instanceof TableNameSubQueryImp) {
				temp = ((TableNameSubQueryImp) tbName).getSubSelect().getSkip(
						param);
				if (temp > skip) {
					skip = temp;
				}
			}
		}
		return skip;
	}

	public void appendSQL(StringBuilder sb) {
		boolean comma = false;
		for (TableName tbName : tbNames) {
			if (comma) {
				sb.append(",");
			}
			comma = true;
			tbName.appendSQL(sb);
		}
		sb.append(" ");
	}

	/**
	 * 除了select中带有group function以外的其他sql(crud都包括)的groupfunctionType都为NORMAL
	 * 
	 * @return
	 */
	public GroupFunctionType getGroupFuncType() {
		return GroupFunctionType.NORMAL;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean comma = false;
		for (TableName tbName : tbNames) {
			if (comma) {
				sb.append(",");
			}
			comma = true;
			if (Constant.useToString(tbName)) {
				sb.append(tbName.toString());
			} else {
				tbName.appendSQL(sb);
			}
		}
		sb.append(" ");
		return sb.toString();
	}

	/**
	 * @param tables
	 * @param args
	 * @param skip
	 *            闭区间，从哪开始
	 * @param max
	 *            开区间，至哪
	 * @return
	 */
	public List<SqlAndTableAtParser> getSqlReadyToRun(
			Collection<Map<String/* 虚拟表名 */, String/* 真实表名 */>> tables,
			List<Object> args, HandlerContainer handlerContainer) {
		if (tables == null) {
			throw new IllegalArgumentException("待替换表名为空");
		}
		if (modifiableList.size() == 0) {
			throw new IllegalArgumentException("未初始化或sql不能直接输出");
		}
		List<SqlAndTableAtParser> retSqls = new ArrayList<SqlAndTableAtParser>(
				tables.size());
		for (Map<String, String> table : tables) {
			Result result = process(table, args, modifiableList, tableName,
					handlerContainer);

			SqlAndTableAtParser sqlAndTableAtParser = new SqlAndTableAtParser();
			sqlAndTableAtParser.sql = result.resultSQL;
			sqlAndTableAtParser.table = table;
			sqlAndTableAtParser.modifiedMap = result.changeParam;
			retSqls.add(sqlAndTableAtParser);
		}
		return retSqls;
	}

	private static class Result {

		public Result(Map<Integer, Object> changeParam, String resultSQL) {
			super();
			this.changeParam = changeParam;
			this.resultSQL = resultSQL;
		}

		final Map<Integer, Object> changeParam;
		final String resultSQL;
	}

	public Result process(Map<String, String> table, List<Object> args,
			List<Object> modifiableTableName, Set<String> originalTable,
			HandlerContainer handlerContainer) {
		// hack.
		boolean allowChangePageNumber = handlerContainer
				.isAllowChangePageNumber();
		if (allowChangePageNumber) {
			PageWrapperCommon skipPage = null;
			// oracle的max或者mysql的range都可以被注入到这里，因为max和range不会同时出现
			// 这部分代码的作用在于先找到最大的起始数值，和结束数值。
			PageWrapperCommon maxOrMaxPage = null;
			for (Object obj : modifiableTableName) {
				if (obj instanceof SkipWrapper) {
					if (skipPage == null) {
						skipPage = (SkipWrapper) obj;
					} else if (skipPage.getVal(args) < ((SkipWrapper) obj)
							.getVal(args)) {
						// 当前值大于snapshot中的值时，与mySelect,Select
						// MyUpdate,Update……里面的参数一致。
						skipPage = (SkipWrapper) obj;
					} else {
						// 当前值小于等于snap中的值，什么也不做。
					}
				} else if (obj instanceof MaxWrapper
						|| obj instanceof RangeWrapper) {
					if (maxOrMaxPage == null) {
						maxOrMaxPage = (PageWrapperCommon) obj;
					} else if (maxOrMaxPage.getVal(args) < ((PageWrapperCommon) obj)
							.getVal(args)) {
						// 当前值大于snapshot中的值时，与mySelect,Select
						// MyUpdate,Update……里面的参数一致
						maxOrMaxPage = (PageWrapperCommon) obj;
					} else {
						// 当前值小于等于snap中的值，什么也不做。
					}
				} else {
					// String 其他的情况
				}
			}
			if (skipPage != null) {
				skipPage.setCanBeChanged(true);
			}
			if (maxOrMaxPage != null) {
				maxOrMaxPage.setCanBeChanged(true);
			}

		}
		Map<Integer, Object> changeParam = null;
		// 替换并输出
		StringBuilder sb = new StringBuilder();
		for (Object obj : modifiableTableName) {
			if (obj instanceof String) {
				// 正常String
				// sb.append(getTable(table,originalTable));
				sb.append(obj.toString());
			} else if (obj instanceof PageWrapper) {
				// 处理分页
				// needAppendtableName=false;
				RangePlaceHandler rangePlaceHandler = handlerContainer
						.getRangePlaceHandler();
				// 初始化俩
				if (changeParam == null) {
					changeParam = new HashMap<Integer, Object>(2);
				}
				String str = rangePlaceHandler.changeValue((PageWrapper) obj,
						changeParam);
				sb.append(str);
			} else if (obj instanceof ReplacableWrapper) {
				PlaceHolderReplaceHandler placeHolderHandler = handlerContainer
						.getPlaceHolderPlaceHandler(obj);
				String replacedTable = placeHolderHandler.getReplacedString(
						table, (ReplacableWrapper) obj);
				// 处理index，表名
				sb.append(replacedTable);

			} else {
				throw new IllegalStateException("should not be here");
			}

		}
		if (changeParam == null)
			changeParam = Collections.emptyMap();
		Result result = new Result(changeParam, sb.toString());
		return result;
	}

	public abstract List<OrderByEle> nestGetOrderByList();

	public abstract List<OrderByEle> nestGetGroupByList();

	// public TableName getTbName() {
	// return tbName;
	// }

	public boolean isDML() {
		return true;
	}

	public void addHint(Hint hint) {
		hints.add(hint);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.taobao.tddl.sqlobjecttree.SqlParserResult#getMax(java.util.List)
	 */
	public int getMax(List<Object> param) {
		// 在默认的情况下，是个mysql的实现，oracle实现在其他子类中
		// TODO:单独抽离出一个MySQLCommon来存放这个逻辑比较清晰。
		int skip = getSkip(param);
		int max = DEFAULT_SKIP_MAX;
		int range = getRangeOrMax(param);
		if (range != DMLCommon.DEFAULT_SKIP_MAX) {
			if (skip != DMLCommon.DEFAULT_SKIP_MAX) {
				if (range >= 0) {
					if (skip >= 0) {
						/*
						 * 目前mysql实现逻辑比较绕,因为mysql的行是从1开始。 但javaList对象的实现从0开始
						 * 因此对于mysql到java到list的对象应该是值++然后--。因此不变
						 * 
						 * oralce中的max值等于mysql中的limit m,n关系中的m+n。
						 */
						max = skip + range;
					} else {
						throw new IllegalArgumentException("skip不允许为负值");
					}
				} else {
					throw new IllegalArgumentException("max或range值不允许为负值");
				}
			} else {
				// 如果没有skip值，则max=range
				max = range;
			}
		}

		if (skip < 0 && skip != DMLCommon.DEFAULT_SKIP_MAX) {
			throw new IllegalArgumentException("skip不允许为负值");
		}
		return max;
	}

	public ComparativeMapChoicer getComparativeMapChoicer() {
		return this;
	}

	/**
	 * FIXME： 已经将Distinct 当做了一个方法， Distinct 和后面的column，一起作为一个Function column,
	 * 此处继续保留这个Distinct，作为一个冗余，是因为后面的分库分表时的代码需要调用这个属性来进行操作!
	 * 此Distinct只作为冗余，根据Parser结果对象方向输出组成SQL的String时将不再输出此Distinct--add by
	 * mazhidan.pt
	 */
	protected Distinct distinct = null;

	public void setDistinct(Distinct distinct) {
		this.distinct = distinct;
	}

	@Override
	public List<String> getDistinctColumn() {
		if (null != distinct) {
			if (null != distinct.getColumns()) {
				return distinct.getColumns().getColList2Str();
			}
		}
			return null;
	}

	@Override
	public boolean hasHavingCondition() {
		return false;
	}

	@Override
	public List<InExpressionObject> getInExpressionObjectList() {
		return this.inObjList;
	}
	
	@Override
	public String getFormatSql() {
		return null;
	}
}
