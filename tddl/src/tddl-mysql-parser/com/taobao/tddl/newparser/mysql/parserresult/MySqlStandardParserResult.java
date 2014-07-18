package com.taobao.tddl.newparser.mysql.parserresult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.cobar.parser.ast.expression.primary.ParamMarker;
import com.alibaba.cobar.parser.ast.fragment.Limit;
import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.taobao.tddl.newparser.mysql.SqlReBuilder;
import com.taobao.tddl.sqlobjecttree.GroupFunctionType;
import com.taobao.tddl.sqlobjecttree.InExpressionObject;
import com.taobao.tddl.sqlobjecttree.OrderByEle;
import com.taobao.tddl.sqlobjecttree.SqlAndTableAtParser;
import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.HandlerContainer;
import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.RangePlaceHandler;

public class MySqlStandardParserResult extends MySqlAbstractParserResult {
	private List<OrderByEle> orderBys = new ArrayList<OrderByEle>();
	private List<OrderByEle> groupBys = new ArrayList<OrderByEle>();
	private AggregateFunction aggregate;
	private Limit limit;
	private boolean hasHaving;
	private List<String> distinctNotFunc = new ArrayList<String>();
	private List<InExpressionObject> inExprObjs = new ArrayList<InExpressionObject>();
	private SQLStatement ast;
	private StringBuilder sqlFormatBuilder = new StringBuilder();

	public void setAst(SQLStatement ast) {
		this.ast = ast;
	}

	public void setAggregate(AggregateFunction aggregate) {
		this.aggregate = aggregate;
	}

	public void setLimit(Limit limit) {
		this.limit = limit;
	}

	public void addOrderBy(OrderByEle orderBy) {
		orderBys.add(orderBy);
	}

	public void addGroupBy(OrderByEle groupBy) {
		groupBys.add(groupBy);
	}

	public void setHasHaving(boolean hasHaving) {
		this.hasHaving = hasHaving;
	}

	public void addDistinctCol(String col) {
		this.distinctNotFunc.add(col);
	}

	public void addInExprObj(InExpressionObject inExprObj) {
		this.inExprObjs.add(inExprObj);
	}

	@Override
	public StringBuilder regTableModifiable(Set<String> logicTableNames,
			List<Object> list, StringBuilder sb) {
		// 分离sql和不可变部分，比如table,limit m,n是可变部分，主要用重新拼出
		// sql并改变里面的table和limit m,n
		// 。这个可以做得更加丰富些,比如where条件可变,id in归组就可以做在这里
		return null;
	}

	@Override
	public int getSkip(List<Object> param) {
		if (this.limit == null) {
			return DEFAULT_SKIP_MAX;
		} else if (limit.getOffset() instanceof ParamMarker) {
			return Integer.valueOf(String.valueOf(param.get(((ParamMarker) limit.getOffset()).getParamIndex() - 1)));
		} else {
			return Integer.valueOf(String.valueOf(limit.getOffset()));
		}
	}

	@Override
	public int getMax(List<Object> param) {
		if (this.limit == null) {
			return DEFAULT_SKIP_MAX;
		} else {
			int skip = getSkip(param);
			if (skip < 0) {
				skip = 0;
			}

			if (limit.getSize() instanceof ParamMarker) {
				return Integer.valueOf(String.valueOf(param.get(((ParamMarker) limit.getSize())
						.getParamIndex() - 1))) + skip;
			} else {
				return Integer.valueOf(String.valueOf(limit.getSize())) + skip;
			}
		}
	}

	@Override
	public GroupFunctionType getGroupFuncType() {
		if (this.aggregate == null) {
			return GroupFunctionType.NORMAL;
		}
		return aggregate.type;
	}

	@Override
	public List<OrderByEle> getOrderByEles() {
		return this.orderBys;
	}

	@Override
	public List<OrderByEle> getGroupByEles() {
		return this.groupBys;
	}

	@Override
	public List<SqlAndTableAtParser> getSqlReadyToRun(
			Collection<Map<String, String>> tables, List<Object> args,
			HandlerContainer handlerContainer) {
		if (tables == null) {
			throw new IllegalArgumentException("待替换表名为空");
		}

		List<SqlAndTableAtParser> retSqls = new ArrayList<SqlAndTableAtParser>(
				tables.size());
		RangePlaceHandler rangePlaceHandler = handlerContainer
				.getRangePlaceHandler();
		for (Map<String, String> table : tables) {
			StringBuilder sql = new StringBuilder();
			SqlReBuilder rebuilder = new SqlReBuilder(sql, table,
					rangePlaceHandler.getMax(), rangePlaceHandler.getSkip());
			ast.accept(rebuilder);
			SqlAndTableAtParser sqlAndTableAtParser = new SqlAndTableAtParser();
			sqlAndTableAtParser.sql = rebuilder.getSql();
			sqlAndTableAtParser.table = table;
			sqlAndTableAtParser.modifiedMap = rebuilder.getChangedParam();
			retSqls.add(sqlAndTableAtParser);
		}

		return retSqls;
	}

	@Override
	public List<String> getDistinctColumn() {
		return this.distinctNotFunc;
	}

	@Override
	public boolean hasHavingCondition() {
		return hasHaving;
	}

	@Override
	public List<InExpressionObject> getInExpressionObjectList() {
		return inExprObjs;
	}

	public StringBuilder getSqlFormatBuilder() {
		return sqlFormatBuilder;
	}
	
	@Override
	public String getFormatSql() {
		return sqlFormatBuilder.toString();
	}
}
