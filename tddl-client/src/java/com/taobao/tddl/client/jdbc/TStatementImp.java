package com.taobao.tddl.client.jdbc;

import static com.taobao.tddl.client.util.ExceptionUtils.appendToExceptionList;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.RouteCondition.ROUTE_TYPE;
import com.taobao.tddl.client.databus.StartInfo;
import com.taobao.tddl.client.jdbc.TDataSource.TDSProperties;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.listener.Context;
import com.taobao.tddl.client.jdbc.listener.Handler;
import com.taobao.tddl.client.jdbc.listener.HookPoints;
import com.taobao.tddl.client.jdbc.resultset.newImp.CountTResultSet;
import com.taobao.tddl.client.jdbc.resultset.newImp.DistinctTResultSet;
import com.taobao.tddl.client.jdbc.resultset.newImp.DummyTResultSet;
import com.taobao.tddl.client.jdbc.resultset.newImp.MaxTResultSet;
import com.taobao.tddl.client.jdbc.resultset.newImp.MinTResultSet;
import com.taobao.tddl.client.jdbc.resultset.newImp.OrderByTResultSet;
import com.taobao.tddl.client.jdbc.resultset.newImp.ShallowTResultSetWrapper;
import com.taobao.tddl.client.jdbc.resultset.newImp.SimpleTResultSet;
import com.taobao.tddl.client.jdbc.resultset.newImp.SumTResultSet;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutorImp;
import com.taobao.tddl.client.jdbc.sqlexecutor.SimpleRealSqlExecutorImp;
import com.taobao.tddl.client.jdbc.sqlexecutor.UpdateReturn;
import com.taobao.tddl.client.jdbc.sqlexecutor.parallel.ParallelRealSqlExecutor;
import com.taobao.tddl.client.jdbc.sqlexecutor.serial.SerialRealSqlExecutor;
import com.taobao.tddl.client.pipeline.bootstrap.Bootstrap;
import com.taobao.tddl.client.util.ExceptionUtils;
import com.taobao.tddl.client.util.ExecutionPlanUtils;
import com.taobao.tddl.client.util.LogUtils;
import com.taobao.tddl.common.channel.SqlMetaData;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.standard.jdbc.TStatement;
import com.taobao.tddl.common.util.SqlTypeParser;
import com.taobao.tddl.interact.monitor.TotalStatMonitor;
import com.taobao.tddl.interact.rule.bean.SqlType;
import com.taobao.tddl.sqlobjecttree.GroupFunctionType;
import com.taobao.tddl.sqlobjecttree.OrderByEle;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;

@SuppressWarnings("unchecked")
public class TStatementImp extends PreparedStatementExecutorCommon implements
		TStatement {
	
	private TDSProperties properties;

	/**
	 * 管线工厂
	 */
	protected final Bootstrap bootstrap;

	protected SerialRealSqlExecutor serialRealSqlExecutor;
	protected ParallelRealSqlExecutor parallelRealSqlExecutor;
	protected SerialRealSqlExecutor simpleSerialRealSqlExecutor;

	private static final Log log = LogFactory.getLog(TStatementImp.class);

	private static final Log sqlLog = LogFactory.getLog(LogUtils.TDDL_SQL_LOG);

	/**
	 * query time out . 超时时间，如果超时时间不为0。那么超时应该被set到真正的query中。
	 */
	protected int queryTimeout = 0;
	protected int maxRows = 0;
	protected int fetchSize = 0;

	/**
	 * 经过计算后的结果集，允许使用 getResult函数调用.
	 * 
	 * 一个statement只允许有一个结果集
	 */
	protected DummyTResultSet currentResultSet;
	/**
	 * 貌似是只有存储过程中会出现多结果集 因此不支持
	 */
	protected boolean moreResults;
	/**
	 * 更新计数，如果执行了多次，那么这个值只会返回最后一次执行的结果。 如果是一个query，那么返回的数据应该是-1
	 */
	protected int updateCount;
	/**
	 * 判断当前statment 是否是关闭的
	 */
	protected boolean closed;

	private int resultSetType = -1;

	private int resultSetConcurrency = -1;

	private int resultSetHoldability = -1;

	protected List<String> batchedArgs;

	private HookPoints hookPoints;
	private Context context;
	
	private SqlMetaData sqlMetaData;

	protected static void dumpSql(String originalSql,
			Map<String, List<RealSqlContext>> targets,
			Map<Integer, ParameterContext> parameters) {
		if (sqlLog.isDebugEnabled()) {
			StringBuilder buffer = new StringBuilder();
			buffer.append("\n[original sql]:").append(originalSql.trim())
					.append("\n");
			for (Entry<String, List<RealSqlContext>> entry : targets.entrySet()) {
				for (RealSqlContext targetSql : entry.getValue()) {
					buffer.append(" [").append(entry.getKey()).append(".")
							.append(targetSql.getRealTable()).append("]:")
							.append(targetSql.getSql().trim()).append("\n");
				}
			}

			if (parameters != null && !parameters.isEmpty()
					&& !parameters.values().isEmpty()) {
				buffer.append("[parameters]:").append(
						parameters.values().toString());
			}

			sqlLog.debug(buffer.toString());
		}

		// 打日志
		for (Entry<String, List<RealSqlContext>> entry : targets.entrySet()) {
			for (RealSqlContext rsc : entry.getValue()) {
				StringBuilder sb = new StringBuilder();
				sb.append(entry.getKey());
				sb.append(TotalStatMonitor.logFieldSep);
				sb.append(rsc.getRealTable());
				TotalStatMonitor.dbTabIncrement(sb.toString());
			}
		}
	}

	public TStatementImp(ConnectionManager connectionManager,
			Bootstrap bootstrap) {
		super(connectionManager);
		this.bootstrap = bootstrap;
	}

	public int executeUpdate(String sql) throws SQLException {
		return executeUpdateInternal(sql, -1, null, null);
	}

	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		return executeUpdateInternal(sql, autoGeneratedKeys, null, null);
	}

	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		return executeUpdateInternal(sql, -1, columnIndexes, null);
	}

	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		return executeUpdateInternal(sql, -1, null, columnNames);
	}

	protected RouteCondition getRouteContiongFromThreadLocal(String key) {
		RouteCondition rc = (RouteCondition) ThreadLocalMap.get(key);
		if (rc != null) {
			ROUTE_TYPE routeType = rc.getRouteType();
			if (ROUTE_TYPE.FLUSH_ON_EXECUTE.equals(routeType)) {
				ThreadLocalMap.put(key, null);
			}
		}
		return rc;
	}

	public void closeInterval(boolean closeInvokeByCurrTStatement)
			throws SQLException {
		if (log.isDebugEnabled()) {
			log.debug("invoke close");
		}
		if (closed) {
			return;
		}

		List<SQLException> exceptions = null;

		closed = true;
		try {
			// 关闭孩子
			try {
				// bug fix by shenxun :内部不让他remove,在TStatment中统一clear掉他们
				if (currentResultSet != null) {
					currentResultSet.closeInternal();
				}

			} catch (SQLException e) {
				exceptions = appendToExceptionList(exceptions, e);
			}
			// @IMPORTANT: 因为目前Statement没有持有使用过的dbIndex的引用，因此在这里不能很好的关闭连接
			// 因为使用多个TStatement的场景太少，所以这个地方的优化是不完备的。在使用多个TStatement的时候，就只能依托于外部显示的调用close()方法了。
		} finally {
			closed = true;
			currentResultSet = null;
			if (closeInvokeByCurrTStatement) {
				connectionManager.removeCurrentStatement(this);
			}
		}
		ExceptionUtils.throwSQLException(exceptions, "close",
				Collections.emptyList());
	}

	/**
	 * update 的核心方法
	 * 
	 * 遵循的几个基本的理论是: 1. 逐级关门 2. 有一个更新失败，那么当前数据源所有更新都失败。 3.
	 * 其他数据库的更新还是会继续执行的，但最后会统一抛出异常。
	 * 
	 * 如果多余一个数据库执行更新，但是是在事务中，那么其他数据库全部会抛事务错误， 这种情况还是不让他发生了
	 * 
	 * @param sql
	 * @param autoGeneratedKeys
	 * @param columnIndexes
	 * @param columnNames
	 * @param sqlParam
	 * @return
	 * @throws SQLException
	 */
	protected int executeUpdateInternal(String sql, int autoGeneratedKeys,
			int[] columnIndexes, String[] columnNames,
			Map<Integer, ParameterContext> sqlParam, SqlType sqlType,
			TStatementImp statementImp) throws SQLException {
		try {
			return executeUpdateInternalInTry(sql, autoGeneratedKeys,
					columnIndexes, columnNames, sqlParam, sqlType, statementImp);
		} finally {
			if (this.connectionManager.getAutoCommit()) {
				this.context.reset();
			}
		}
	}

	protected int executeUpdateInternalInTry(String sql, int autoGeneratedKeys,
			int[] columnIndexes, String[] columnNames,
			Map<Integer, ParameterContext> sqlParam, SqlType sqlType,
			TStatementImp tStatementImp) throws SQLException {

		checkClosed();
		ensureResultSetIsEmpty();
		long startTime = System.currentTimeMillis();
		ExecutionPlan context = buildSqlExecutionContextUsePipeline(sql,
				sqlParam, sqlType);
		fillMetaData(context.getSqlMetaData());

		if (context.getEvents() != null) {
			this.context.getEvents().addAll(context.getEvents());
		}

		if (context.mappingRuleReturnNullValue()) {
			return 0;
		}

		beforeSqlExecute();

		int tablesSize = 0;
		Map<String, List<RealSqlContext>> sqlMap = context.getSqlMap();
		int databaseSize = sqlMap.size();

		dumpSql(sql, sqlMap, null);

		int affectedRows = 0;

		List<SQLException> exceptions = new LinkedList<SQLException>();
		Set<Entry<String, List<RealSqlContext>>> set = sqlMap.entrySet();

		RealSqlExecutor rse = new RealSqlExecutorImp(parallelRealSqlExecutor,
				serialRealSqlExecutor, tStatementImp, context);

		context.setAutoGeneratedKeys(autoGeneratedKeys);
		context.setColumnIndexes(columnIndexes);
		context.setColumnNames(columnNames);

		for (Entry<String/* dbIndex */, List<RealSqlContext>> entry : set) {
			UpdateReturn ur = null;
			try {
				ur = rse.update();
			} catch (SQLException e) {
				exceptions.add(e);
				break;
			}

			affectedRows += ur.getAffectedRows();
			exceptions.addAll(ur.getExceptions());
			tablesSize += entry.getValue().size();
		}

		long elapsedTime = System.currentTimeMillis() - startTime;

		ExceptionUtils.throwSQLException(exceptions, sql, sqlParam);

		this.currentResultSet = null;
		this.moreResults = false;
		this.updateCount = affectedRows;
		this.context.setAffectedRows(affectedRows);
		profileUpdate(sql, context, tablesSize, databaseSize, exceptions,
				elapsedTime);

		afterSqlExecute();
		return affectedRows;
	}

	// update不需要重试
	private int executeUpdateInternal(String sql, int autoGeneratedKeys,
			int[] columnIndexes, String[] columnNames) throws SQLException {
		SqlType sqlType = SqlTypeParser.getSqlType(sql);
		return executeUpdateInternal(sql, autoGeneratedKeys, columnIndexes,
				columnNames, null, sqlType, this);
	}

	protected void profileUpdate(String sql, ExecutionPlan context,
			int tablesSize, int databaseSize, List<SQLException> exceptions,
			long elapsedTime) throws SQLException {
		profileWithException(exceptions, context.getVirtualTableName()
				.toString(), sql, elapsedTime);
		profileNumberOfDBAndTablesAndDuringTime(context.getVirtualTableName()
				.toString(), databaseSize, tablesSize, sql, elapsedTime);

	}

	protected void afterSqlExecute() throws SQLException {
		if (connectionManager.getAutoCommit()) {
			// 记录一下执行分库复制以前的时间。
			if (hookPoints.getAfterExecute() != Handler.DUMMY_HANDLER
					&& !context.isEventsEmpty()) {
				for (SqlExecuteEvent event : context.getEvents()) {
					event.setAfterMainDBSqlExecuteTime(System
							.currentTimeMillis());
				}
			}
			hookPoints.getAfterExecute().execute(context);
		}
	}

	private Statement createStatementInternal(Connection connection)
			throws SQLException {
		Statement stmt;
		if (this.resultSetType != -1 && this.resultSetConcurrency != -1
				&& this.resultSetHoldability != -1) {
			stmt = connection.createStatement(this.resultSetType,
					this.resultSetConcurrency, this.resultSetHoldability);
		} else if (this.resultSetType != -1 && this.resultSetConcurrency != -1) {
			stmt = connection.createStatement(this.resultSetType,
					this.resultSetConcurrency);
		} else {
			stmt = connection.createStatement();
		}
		return stmt;
	}

	private boolean executeInternal(String sql, int autoGeneratedKeys,
			int[] columnIndexes, String[] columnNames) throws SQLException {
		if (SqlTypeParser.isQuerySql(sql)) {
			executeQuery(sql);
			return true;
		} 
		else {
			if (autoGeneratedKeys == -1 && columnIndexes == null
					&& columnNames == null) {
				executeUpdate(sql);
			} else if (autoGeneratedKeys != -1) {
				executeUpdate(sql, autoGeneratedKeys);
			} else if (columnIndexes != null) {
				executeUpdate(sql, columnIndexes);
			} else if (columnNames != null) {
				executeUpdate(sql, columnNames);
			} else {
				executeUpdate(sql);
			}
			return false;
		}
	}

	public boolean execute(String sql) throws SQLException {
		return executeInternal(sql, -1, null, null);
	}

	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		return executeInternal(sql, autoGeneratedKeys, null, null);
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return executeInternal(sql, -1, columnIndexes, null);
	}

	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		return executeInternal(sql, -1, null, columnNames);
	}

	public void addBatch(String sql) throws SQLException {
		checkClosed();
		if (batchedArgs == null) {
			batchedArgs = new LinkedList<String>();
		}
		if (sql != null) {
			batchedArgs.add(sql);
		}
	}

	public void clearBatch() throws SQLException {
		checkClosed();
		if (batchedArgs != null) {
			batchedArgs.clear();
		}
	}

	public void close() throws SQLException {
		closeInterval(true);
	}

	public int[] executeBatch() throws SQLException {
		checkClosed();
		ensureResultSetIsEmpty();
		// 这个地方等到执行计划出来之后，发现确实有跨库的事务再予以拒绝。
		// if (!connectionManager.getAutoCommit()) {
		// throw new SQLException("executeBatch暂不支持事务");
		// }
		if (batchedArgs == null || batchedArgs.isEmpty()) {
			return new int[0];
		}

		List<SQLException> exceptions = new ArrayList<SQLException>();
		List<Integer> result = new ArrayList<Integer>();
		Map<String/* 数据源ID */, List<String/* 数据源上执行的SQL */>> sqls = null;
		try {
			DirectlyRouteCondition ruleCondition = (DirectlyRouteCondition) getRouteContiongFromThreadLocal(ThreadLocalString.RULE_SELECTOR);

			// if (directlyRouteCondition != null) {
			// // 是直接路由的condition
			// String dbRuleId = directlyRouteCondition.getDbRuleID();
			// if (connectionManager.containDBIndex(dbRuleId)) {
			// // 这里是直接执行sql
			// throw new SQLException("batch not support");
			// } else {
			// // 不包含目标id
			// // 那么判断一下当前rc里面是否有需要替换的表名，如果有需要替换的
			// // 表名，则抛出异常，目标数据库未找到，如果没有要替换的表名。则走规则选择
			// if
			// (directlyRouteCondition.getShardTableMap().get(dbRuleId).isEmpty())
			// {
			// sqls = sortBatch(batchedArgs, dbRuleId);
			// } else {
			// throw new SQLException("can't find target db : "
			// + dbRuleId);
			// }
			// }

			if (ruleCondition != null) {
				String dbRuleId = ruleCondition.getDbRuleID();
				sqls = sortBatch(batchedArgs, dbRuleId);
			} else {
				sqls = sortBatch(batchedArgs, null);
			}
			// add by jiechen.qzm batch不支持跨库事务
			if (sqls.size() > 1 && !connectionManager.getAutoCommit()) {
				throw new SQLException("executeBatch暂不支持跨库事务，该事务涉及 "
						+ sqls.size() + " 个库。");
			}

			for (Entry<String, List<String>> entry : sqls.entrySet()) {
				String dbSelectorID = entry.getKey();
				List<Integer> list = null;
				try {
					// 这里显示的使用go slave为false
					// 拉屎关门
					Connection conn = connectionManager.getConnection(
							dbSelectorID, false);
					try {
						list = executeBatchOnOneConnAndCloseStatement(
								exceptions, entry.getValue(), conn);
						result.addAll(list);
					} finally {
						exceptions = tryCloseConnection(exceptions,
								dbSelectorID);
					}
				} catch (SQLException e) {
					exceptions = appendToExceptionList(exceptions, e);
				}
			}
		} finally {
			batchedArgs.clear();
		}
		currentResultSet = null;
		moreResults = false;
		updateCount = 0;
		ExceptionUtils.throwSQLException(exceptions, "batch",
				Collections.EMPTY_MAP);

		return fromListToArray(result);
		// return new int[0];
	}

	public Map<String, List<String>> sortBatch(List<String> sql,
			String selectKey) throws SQLException {
		Map<String, List<String>> targetSqls = new HashMap<String, List<String>>(
				8);
		StartInfo startInfo = new StartInfo();
		for (String originalSql : sql) {
			startInfo.setSql(originalSql);
			startInfo
					.setSqlType(SqlTypeParser.getSqlType(originalSql));
			bootstrap
					.bootstrapForBatch(startInfo, false, targetSqls, selectKey);
		}
		return targetSqls;
	}

	public Map<String, Map<String, List<List<ParameterContext>>>> sortPreparedBatch(
			String sql, List<Map<Integer, ParameterContext>> batchedParameters,
			String selectKey) throws SQLException {
		Map<String, Map<String, List<List<ParameterContext>>>> targetSqls = new HashMap<String, Map<String, List<List<ParameterContext>>>>(
				16);
		StartInfo startInfo = new StartInfo();
		startInfo.setSql(sql);
		startInfo.setSqlType(SqlTypeParser.getSqlType(sql));
		for (Map<Integer, ParameterContext> map : batchedParameters) {
			startInfo.setSqlParam(map);
			bootstrap.bootstrapForPrepareBatch(startInfo, false, targetSqls,
					selectKey);
		}
		return targetSqls;
	}

	/**
	 * 这个方法先被注释掉，因为无法返回 batch的结果，只有SqlException是不够的。
	 * 
	 * @param exceptions
	 * @param sqls
	 * @param conn
	 * @return
	 */
	/*
	 * protected List<SQLException> executeBatchOnOneConnAndCloseStatement(
	 * List<SQLException> exceptions, List<String> sqls, Connection conn) { try
	 * { Statement stmt = createStatementInternal(conn);
	 * 
	 * try { try { for (String targetSql : sqls) { stmt.addBatch(targetSql); }
	 * stmt.executeBatch(); } catch (SQLException e) { exceptions =
	 * appendToExceptionList(exceptions, e); } stmt.clearBatch(); } finally {
	 * stmt.close(); } } catch (SQLException e) { exceptions =
	 * appendToExceptionList(exceptions, e); } return exceptions; }
	 */

	/**
	 * 返回影响数量的list,多条sql的话直接append
	 * 
	 * @param exceptions
	 * @param sqls
	 * @param conn
	 * @return
	 */
	protected List<Integer> executeBatchOnOneConnAndCloseStatement(
			List<SQLException> exceptions, List<String> sqls, Connection conn) {
		List<Integer> result = new ArrayList<Integer>();
		try {
			Statement stmt = createStatementInternal(conn);
			try {
				try {
					int[] temp = null;
					for (String targetSql : sqls) {
						stmt.addBatch(targetSql);
					}
					temp = stmt.executeBatch();
					result.addAll(fromArrayToList(temp));
				} catch (SQLException e) {
					exceptions = appendToExceptionList(exceptions, e);
				}
				stmt.clearBatch();
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			exceptions = appendToExceptionList(exceptions, e);
		}
		return result;
	}

	/**
	 * 从 int[] 到 List<Integer> 的准换
	 * 
	 * @param array
	 * @return
	 */
	public static List<Integer> fromArrayToList(int[] array) {
		if (array == null) {
			return null;
		}
		List<Integer> result = new ArrayList<Integer>();
		for (int num : array) {
			result.add(num);
		}
		return result;
	}

	/**
	 * 从 List<Integer> 到 int[] 的准换
	 * 
	 * @param list
	 * @return
	 */
	public static int[] fromListToArray(List<Integer> list) {
		if (list == null) {
			return null;
		}
		int[] result = new int[list.size()];
		for (int i = 0; i < list.size(); i++) {
			result[i] = list.get(i);
		}
		return result;
	}

	protected void checkClosed() throws SQLException {
		if (closed) {
			throw new SQLException(
					"No operations allowed after statement closed.");
		}
	}

	/**
	 * 如果新建了查询，那么上一次查询的结果集应该被显示的关闭掉。这才是符合jdbc规范的
	 * 
	 * @throws SQLException
	 */
	protected void ensureResultSetIsEmpty() throws SQLException {

		if (currentResultSet != null) {
			log.debug("result set is not null,close current result set");
			try {
				currentResultSet.close();
			} catch (SQLException e) {
				log.error(
						"exception on close last result set . can do nothing..",
						e);
			} finally {
				// 最终要显示的关闭它
				currentResultSet = null;
			}
		}
	}

	protected ResultSet executeQueryInternal(String sql,
			Map<Integer, ParameterContext> originalParameterSettings,
			SqlType sqlType, TStatementImp tStatementImp) throws SQLException {
		checkClosed();
		ensureResultSetIsEmpty();
		ExecutionPlan context = null;
		context = buildSqlExecutionContextUsePipeline(sql,
				originalParameterSettings, sqlType);
		fillMetaData(context.getSqlMetaData());

		/*
		 * modified by shenxun: 这里主要是处理mappingRule返回空的情况下，应该返回空结果集
		 */
		if (context.mappingRuleReturnNullValue()) {
			this.currentResultSet = getEmptyResultSet(this);
			return currentResultSet;
		}
		// int tablesSize = 0;
		dumpSql(sql, context.getSqlMap(), originalParameterSettings);

		// beforeSqlExecute();

		DummyTResultSet result = null;
		// 这里允许抛出异常，有异常表示查尽结果集仍没有一个可返回的
		result = mergeResultSets(this, connectionManager, context);

		this.currentResultSet = result;
		this.moreResults = false;
		this.updateCount = -1;
		// 记录打开的resultSet,用于一起关闭时能找得到

		// afterSqlExecute();
		if (connectionManager.getAutoCommit()) {
			this.context.reset();
		}
		return result;
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		SqlType sqlType = SqlTypeParser.getSqlType(sql);
		return executeQueryInternal(sql, null, sqlType, this);
	}

	// TODO : 使用策略模式进行合并，以支持并发查询和非并发查询
	protected DummyTResultSet mergeResultSets(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan context)
			throws SQLException {

		if (context.getOrderByColumns() != null
				&& !context.getOrderByColumns().isEmpty()
				&& context.getGroupFunctionType() != GroupFunctionType.NORMAL) {
			throw new SQLException(
					"'group function' and 'order by' can't be together!");
		}
		// 如果是有切仅有一个数据源，并且只查一个表，那么走最简单的模式！
		/*
		 * 简单模式就是直接去数据库执行，什么都不做，这种模式下可以支持任何查询，不需要合并，不需要
		 * 搞其他事情。就是去数据库查询，然后记下statement记下resultSet.即可，所有操作 直接通过结果集进行。不需要做额外判断。
		 */
		if (ExecutionPlanUtils.isOneSqlPlan(context)) {
			return new ShallowTResultSetWrapper(tStatementImp,
					connectionManager, context);
		}

		RealSqlExecutor rse = new RealSqlExecutorImp(parallelRealSqlExecutor,
				serialRealSqlExecutor, tStatementImp, context);

		// 如果是ave那么不能支持
		if (context.getGroupFunctionType() == GroupFunctionType.AVG) {
			throw new SQLException(
					"The group function 'AVG' is not supported now!");
		} else if (context.getGroupFunctionType() == GroupFunctionType.COUNT) {
			// 如果是count则进行合并
			return new CountTResultSet(tStatementImp, connectionManager,
					context, rse);
		} else if (context.getGroupFunctionType() == GroupFunctionType.MAX) {
			// 如果是
			return new MaxTResultSet(tStatementImp, connectionManager, context,
					rse);
		} else if (context.getGroupFunctionType() == GroupFunctionType.MIN) {
			return new MinTResultSet(tStatementImp, connectionManager, context,
					rse);
		} else if (context.getGroupFunctionType() == GroupFunctionType.SUM) {
			return new SumTResultSet(tStatementImp, connectionManager, context,
					rse);
		} else if (context.getDistinctColumns() != null
				&& context.getDistinctColumns().size() != 0) {
			// 此时肯定已经是单库多表或者多库多表case了,
			DistinctTResultSet rs = new DistinctTResultSet(tStatementImp,
					connectionManager, context, rse);
			rs.setDistinctColumn(context.getDistinctColumns());
			return rs;
		} else if (context.getOrderByColumns() != null
				&& !context.getOrderByColumns().isEmpty()) {
			OrderByColumn[] orderByColumns = new OrderByColumn[context
					.getOrderByColumns().size()];
			int i = 0;
			for (OrderByEle element : context.getOrderByColumns()) {
				orderByColumns[i] = new OrderByColumn();
				orderByColumns[i].setColumnName(element.getName());
				orderByColumns[i++].setAsc(element.isASC());
			}
			OrderByTResultSet orderByTResultSet = new OrderByTResultSet(
					tStatementImp, connectionManager, context, rse);
			orderByTResultSet.setOrderByColumns(orderByColumns);
			orderByTResultSet.setLimitFrom(context.getSkip());
			orderByTResultSet.setLimitTo(context.getMax());
			return orderByTResultSet;
		} else {
			/**
			 * 这个有点特殊
			 */
			RealSqlExecutor spe = new SimpleRealSqlExecutorImp(
					parallelRealSqlExecutor, simpleSerialRealSqlExecutor,
					tStatementImp, context);
			SimpleTResultSet simpleTResultSet = new SimpleTResultSet(
					tStatementImp, connectionManager, context, spe);
			simpleTResultSet.setLimitFrom(context.getSkip());
			simpleTResultSet.setLimitTo(context.getMax());
			return simpleTResultSet;
		}
		// 暂时排除走多库多表的GroupBy,Having,Distinct
	}

	protected void beforeSqlExecute() throws SQLException {
		if (connectionManager.getAutoCommit()) {
			hookPoints.getBeforeExecute().execute(context);
		}
	}

	public Connection getConnection() throws SQLException {
		return connectionManager.getProxyConnection();
	}

	private ExecutionPlan buildSqlExecutionContextUsePipeline(String sql,
			Map<Integer, ParameterContext> originalParameterSettings,
			SqlType sqlType) throws SQLException {
		StartInfo startInfo = new StartInfo();
		startInfo.setSql(sql);
		startInfo.setSqlType(sqlType);
		startInfo.setSqlParam(originalParameterSettings);
		return this.bootstrap.bootstrap(startInfo);
	}

	/**
	 * 以下为不支持的方法
	 */
	public int getFetchDirection() throws SQLException {
		throw new UnsupportedOperationException("getFetchDirection");
	}

	public int getFetchSize() throws SQLException {
		return this.fetchSize;
	}

	public int getMaxFieldSize() throws SQLException {
		throw new UnsupportedOperationException("getMaxFieldSize");
	}

	public int getMaxRows() throws SQLException {
		return this.maxRows;
	}

	public boolean getMoreResults() throws SQLException {
		return moreResults;
	}

	public int getQueryTimeout() throws SQLException {
		return queryTimeout;
	}

	public void setQueryTimeout(int queryTimeout) throws SQLException {
		this.queryTimeout = queryTimeout;
	}

	public void setCursorName(String cursorName) throws SQLException {
		throw new UnsupportedOperationException("setCursorName");
	}

	public void setEscapeProcessing(boolean escapeProcessing)
			throws SQLException {
		throw new UnsupportedOperationException("setEscapeProcessing");
	}

	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	public void clearWarnings() throws SQLException {
	}

	public boolean getMoreResults(int current) throws SQLException {
		throw new UnsupportedOperationException("getMoreResults");
	}

	public ResultSet getResultSet() throws SQLException {
		return currentResultSet;
	}

	public int getResultSetConcurrency() throws SQLException {
		return resultSetConcurrency;
	}

	public int getResultSetHoldability() throws SQLException {
		return resultSetHoldability;
	}

	public int getResultSetType() throws SQLException {
		return resultSetType;
	}

	public int getUpdateCount() throws SQLException {
		return updateCount;
	}

	public void setFetchDirection(int fetchDirection) throws SQLException {
		throw new UnsupportedOperationException("setFetchDirection");
	}

	public void setFetchSize(int fetchSize) throws SQLException {
		this.fetchSize = fetchSize;
	}

	public void setMaxFieldSize(int maxFieldSize) throws SQLException {
		throw new UnsupportedOperationException("setMaxFieldSize");
	}

	public void setMaxRows(int maxRows) throws SQLException {
		this.maxRows = maxRows;
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		throw new UnsupportedOperationException("getGeneratedKeys");
	}

	public void cancel() throws SQLException {
		throw new UnsupportedOperationException("cancel");
	}

	public boolean isCurrentRSClosedOrNull() {
		return currentResultSet == null ? true : currentResultSet.isClosed();
	}

	public void setHookPoints(HookPoints hookPoints) {
		this.hookPoints = hookPoints;
	}

	public HookPoints getHookPoints() {
		return hookPoints;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public Context getContext() {
		return context;
	}

	public int getQueryTimeOut() {
		return queryTimeout;
	}

	public void setResultSetType(int resultSetType) {
		this.resultSetType = resultSetType;
	}

	public void setResultSetConcurrency(int resultSetConcurrency) {
		this.resultSetConcurrency = resultSetConcurrency;
	}

	public void setResultSetHoldability(int resultSetHoldability) {
		this.resultSetHoldability = resultSetHoldability;
	}

	public TDSProperties getProperties() {
		return properties;
	}

	public void setProperties(TDSProperties properties) {
		this.properties = properties;
	}

	public void setSerialRealSqlExecutor(
			SerialRealSqlExecutor serialRealSqlExecutor) {
		this.serialRealSqlExecutor = serialRealSqlExecutor;
	}

	public void setParallelRealSqlExecutor(
			ParallelRealSqlExecutor parallelRealSqlExecutor) {
		this.parallelRealSqlExecutor = parallelRealSqlExecutor;
	}

	public void setSimpleSerialRealSqlExecutor(
			SerialRealSqlExecutor simpleSerialRealSqlExecutor) {
		this.simpleSerialRealSqlExecutor = simpleSerialRealSqlExecutor;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return this.getClass().isAssignableFrom(iface);
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		try {
			return (T) this;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLException("not support exception");
	}

	public boolean isPoolable() throws SQLException {
		throw new SQLException("not support exception");
	}

	@Override
	public void fillMetaData(SqlMetaData sqlMetaData) {
		this.sqlMetaData = sqlMetaData;
	}
	
	@Override
	public SqlMetaData getSqlMetaData() {
		return sqlMetaData;
	}
}
