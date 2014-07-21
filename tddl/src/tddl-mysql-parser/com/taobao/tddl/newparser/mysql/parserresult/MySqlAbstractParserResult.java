package com.taobao.tddl.newparser.mysql.parserresult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeAND;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.sqlobjecttree.GroupFunctionType;
import com.taobao.tddl.sqlobjecttree.InExpressionObject;
import com.taobao.tddl.sqlobjecttree.OrderByEle;
import com.taobao.tddl.sqlobjecttree.SqlAndTableAtParser;
import com.taobao.tddl.sqlobjecttree.TableName;
import com.taobao.tddl.sqlobjecttree.common.value.BindVar;
import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.HandlerContainer;

public abstract class MySqlAbstractParserResult implements MySqlParserResult {
	public final static int DEFAULT_SKIP_MAX = -1000;
	private final List<TableName> tableNames = new ArrayList<TableName>(2);
	private boolean isDML = true;
	private String parsedSql;
	private Map<String/* up case column name */, List<Comparative>> columnMap = new HashMap<String, List<Comparative>>();
	private List<MySqlAbstractParserResult> subQuerys = new ArrayList<MySqlAbstractParserResult>();

	public void addTableName(TableName tableName) {
		this.tableNames.add(tableName);
	}

	public void setDML(boolean isDML) {
		this.isDML = isDML;
	}

	public void addColumn(String colName, Comparative value) {
		List<Comparative> c = this.columnMap.get(colName.toUpperCase());
		if (c != null) {
			c.add(value);
		} else {
			c = new ArrayList<Comparative>();
			c.add(value);
			this.columnMap.put(colName.toUpperCase(), c);
		}
	}

	public void setParsedSql(String parsedSql) {
		this.parsedSql = parsedSql;
	}

	public void addSubQuery(MySqlAbstractParserResult subQuery) {
		this.subQuerys.add(subQuery);
	}

	@Override
	public boolean isDML() {
		return isDML;
	}

	@Override
	public void appendSQL(StringBuilder sb) {
		sb.append(parsedSql);
	}

	@Override
	public Set<String> getTableName() {
		Set<String> tbs = new HashSet<String>();
		for (TableName tn : tableNames) {
			tbs.addAll(tn.getTableName());
		}
		return tbs;
	}

	@Override
	public ComparativeMapChoicer getComparativeMapChoicer() {
		return this;
	}

	@Override
	public Map<String, Comparative> getColumnsMap(List<Object> arguments,
			Set<String> partnationSet) {
		Map<String, Comparative> shardKeyMap = new HashMap<String, Comparative>(
				partnationSet.size());
		for (String shardKey : partnationSet) {
			// 分库分表字段可能出现在各个地方，同一个字段必须值相同。
			List<Comparative> coms = columnMap.get(shardKey.toUpperCase());
			Comparative temp = null;
			if(coms==null){
				continue;
			}
			
			for (Comparative com : coms) {
				if (com != null) {
					if (com instanceof ComparativeOR) {
						valueReplaceComparativeList(
								((ComparativeOR) com).getList(), arguments);
					} else if (com instanceof ComparativeAND) {
						valueReplaceComparativeList(
								((ComparativeAND) com).getList(), arguments);
					} else if (com instanceof Comparative) {
						valueReplaceComparative(com, arguments);
					}

					comparativeEqual(temp, com);
					temp = com;
					shardKeyMap.put(shardKey, com);
				}
			}
		}

		return shardKeyMap;
	}

	private void comparativeEqual(Comparative temp, Comparative com) {
		// 这个逻辑只会在分库分表字段在sql中出现多次才会进入到判断中
		if (temp != null
				&& ((com instanceof ComparativeOR || com instanceof ComparativeAND) || (temp instanceof ComparativeOR || temp instanceof ComparativeAND))) {
			throw new RuntimeException(
					"not support shard key condition apprear more than one place with 'and' or 'or'!condition is:"
							+ temp.toString() + "," + com.toString());
		} else if (temp != null) {
			if (temp instanceof Comparative && com instanceof Comparative) {
				if (temp.getComparison() != com.getComparison()
						|| temp.getValue() != com.getValue()) {
					throw new RuntimeException(
							"not support shard key condition apprear more than one place with different condition!condition is:"
									+ temp.toString() + "," + com.toString());
				}
			}
		}
	}

	private void valueReplaceComparativeList(List<Comparative> coms,
			List<Object> arguments) {
		for (Comparative c : coms) {
			valueReplaceComparative(c, arguments);
		}
	}

	@SuppressWarnings("rawtypes")
	private void valueReplaceComparative(Comparative c, List<Object> arguments) {
		if (c.getValue() instanceof Comparative) {
			valueReplaceComparative((Comparative) c.getValue(), arguments);
		} else if (c.getValue() instanceof BindVar) {
			Comparable val = (Comparable) arguments.get(((BindVar) (c
					.getValue())).getIndex());
			c.setValue(val);
		} else {
		}
	}

	@Override
	public Comparative getColumnComparative(List<Object> arguments,
			String colName) {
		List<Comparative> coms = columnMap.get(colName.toUpperCase());
		Comparative temp = null;
		if (coms == null) {
			return null;
		}

		//多处出现必须一致，所以任选一个
		for (Comparative com : coms) {
			if (com != null) {
				if (com instanceof ComparativeOR) {
					valueReplaceComparativeList(
							((ComparativeOR) com).getList(), arguments);
				} else if (com instanceof ComparativeAND) {
					valueReplaceComparativeList(
							((ComparativeAND) com).getList(), arguments);
				} else if (com instanceof Comparative) {
					valueReplaceComparative(com, arguments);
				} else {
					// can not here;
				}
				comparativeEqual(temp, com);
				temp = com;
			}
		}

		return temp;
	}

	@Override
	public StringBuilder regTableModifiable(Set<String> logicTableNames,
			List<Object> list, StringBuilder sb) {
		return null;
	}

	@Override
	public int getSkip(List<Object> param) {
		return DEFAULT_SKIP_MAX;
	}

	@Override
	public int getMax(List<Object> param) {
		return DEFAULT_SKIP_MAX;
	}

	@Override
	public GroupFunctionType getGroupFuncType() {
		return GroupFunctionType.NORMAL;
	}

	@Override
	public List<OrderByEle> getOrderByEles() {
		return new ArrayList<OrderByEle>();
	}

	@Override
	public List<OrderByEle> getGroupByEles() {
		return new ArrayList<OrderByEle>();
	}

	@Override
	public List<SqlAndTableAtParser> getSqlReadyToRun(
			Collection<Map<String, String>> tables, List<Object> args,
			HandlerContainer handlerContainer) {
		return null;
	}

	@Override
	public List<String> getDistinctColumn() {
		return new ArrayList<String>();
	}

	@Override
	public boolean hasHavingCondition() {
		return false;
	}

	@Override
	public List<InExpressionObject> getInExpressionObjectList() {
		return new ArrayList<InExpressionObject>();
	}
}
