package com.taobao.tddl.client.jdbc.resultset.newImp;

import static com.taobao.tddl.client.util.ExceptionUtils.appendToExceptionList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.RealSqlContext;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.sqlexecutor.QueryReturn;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;
import com.taobao.tddl.client.util.ExceptionUtils;

/**
 * @author guangxia
 * @author junyu
 *
 */
public class PlainAbstractTResultSet extends ProxyTResultSet {
	public PlainAbstractTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,
			RealSqlExecutor realSqlExecutor) throws SQLException {
		this(tStatementImp, connectionManager, executionPlan, realSqlExecutor,
				true);
		super.setResultSetProperty(tStatementImp);
	}

	/**
	 * 测试留下的后门。允许不调用init方法
	 *
	 * @param tStatementImp
	 * @param connectionManager
	 * @param executionPlan
	 * @param init
	 * @throws SQLException
	 */
	public PlainAbstractTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,
			RealSqlExecutor realSqlExecutor, boolean init) throws SQLException {
		super(connectionManager);
		this.tStatementImp = tStatementImp;
		this.realSqlExecutor = realSqlExecutor;
		this.executionPlan = executionPlan;
		if (init) {
			init(connectionManager, executionPlan);
		}
	}

	private long startQueryTime = 0;
	/**
	 * 是否允许验证所有的库和表
	 */
	protected boolean enableProfileRealDBAndTables;

	/**
	 * 执行计划
	 */
	protected final ExecutionPlan executionPlan;

	/**
	 * 当前持有的真正的结果集
	 */
	protected List<ResultSet> actualResultSets;
	/**
	 * 当前持有的所有statement
	 */
	protected Set<Statement> actualStatements;

	/**
	 * 谁新建了当前结果集？
	 */
	protected final TStatementImp tStatementImp;

	/**
	 * sql执行器
	 */
	protected final RealSqlExecutor realSqlExecutor;

	/**
	 * 初始化的方法， 这个方法可以初始化数据源，默认的实现是 与TStatement的重试一致的一套重试机制。目的是让一些必须打开全表的查询可以兼容。
	 *
	 *
	 * @param connectionManager
	 * @param executionContext
	 * @throws SQLException
	 */
	protected void init(ConnectionManager connectionManager,
			ExecutionPlan context) throws SQLException {
		startQueryTime = System.currentTimeMillis();
		checkClosed();
		Map<String/* db Selector id */, List<RealSqlContext>/* 真正在当前database上执行的sql的列表 */> sqlMap = context
				.getSqlMap();
		// 先计算一下表的总个数，这样初始化的时候可以省些力气
		int tableSize = 0;
		for (List<RealSqlContext> l : sqlMap.values()) {
			tableSize += l.size();
		}

		List<SQLException> exceptions = new LinkedList<SQLException>();
		actualResultSets = new ArrayList<ResultSet>(tableSize);
		actualStatements = new HashSet<Statement>(tableSize);

		// 真正的进行查询操作了。
		boolean needBreak = false;
		for (Entry<String, List<RealSqlContext>> dbEntry : sqlMap.entrySet()) {
			if (needBreak) {
				break;
			}
			List<RealSqlContext> sqlList = dbEntry.getValue();
			for (int i=0;i<sqlList.size();i++) {
				QueryReturn qr = null;
				try {
					qr = this.realSqlExecutor.query();
				} catch (SQLException e) {
					exceptions.add(e);
					break;
				}
				if (qr != null) {
					if (null == qr.getExceptions()) {
						actualResultSets.add(qr.getResultset());
						actualStatements.add(qr.getStatement());
					} else {
						exceptions = appendToExceptionList(exceptions,
								qr.getExceptions());
					}
				} else {
					needBreak = true;
					break;
				}
			}
		}

		int databaseSize = sqlMap.size();

		// 查询中只对db tab进行统计。抛异常也单独统计
		profileNumberOfDBAndTablesOnly(
				context.getVirtualTableName().toString(), databaseSize,
				tableSize, context.getOriginalSql());

		ExceptionUtils.throwSQLException(exceptions, context.getOriginalSql(),
				context.getOriginalArgs());
	}

	private static final Log log = LogFactory
			.getLog(PlainAbstractTResultSet.class);

	/**
	 * bug fix by shenxun : 原来会发生一个情况就是如果TStatement调用了close()方法
	 * 而本身其管理的TResultSet没有closed时候。外部会使用iterator来遍历每一个
	 * TResultSet，调用关闭的方法，但因为TResultSet的close方法会回调
	 * TStatement里面用于创建iterator的Set<ResultSet>对象，并使用remove方法。
	 * 这就会抛出一个concurrentModificationException。
	 *
	 * @param removeThis
	 *            目前在TResultSet中，是否为true都不会影响任何事情了
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public void closeInternal() throws SQLException {
		List<SQLException> exceptions = null;
		if (log.isDebugEnabled()) {
			log.debug("invoke close");
		}

		if (closed) {
			return;
		}

		/**
		 * 防止查到一半放弃查询
		 */
		realSqlExecutor.clearQueryResource();

		// 统计整个查询的耗时。或许不是很准，但比较重要。
		long elapsedTime = System.currentTimeMillis() - startQueryTime;

		profileDuringTime(exceptions, executionPlan.getVirtualTableName()
				.toString(), executionPlan.getOriginalSql(), elapsedTime);

		try {
			// 关闭resultset
			for (ResultSet rs : actualResultSets) {
				try {
					rs.close();
				} catch (SQLException e) {
					exceptions = appendToExceptionList(exceptions, e);
				}
			}

			// 关闭statement
			for (Statement stmt : actualStatements) {
				try {
					stmt.close();
				} catch (SQLException e) {
					exceptions = appendToExceptionList(exceptions, e);
				}
			}
		} finally {
			closed = true;
			actualStatements.clear();
			actualResultSets.clear();
			// 不需要移除当前resultSet从父类，因为子类只是关闭，还不需要移除。
			// if (removeThis) {
			// tStatementImp.removeCurrentTResultSet(this);
			// }
		}
		// 通知父类关闭所有连接
		for (String key : executionPlan.getSqlMap().keySet()) {
			exceptions = tryCloseConnection(exceptions, key);
		}
		// 抛出异常，如果exception 不为null
		ExceptionUtils.throwSQLException(exceptions,
				"sql exception during close resources", Collections.EMPTY_LIST);

	}

	protected void checkClosed() throws SQLException {
		if (closed) {
			throw new SQLException(
					"No operations allowed after resultset closed.");
		}
	}

	@Override
	public Statement getStatement() throws SQLException {
		return tStatementImp;
	}

	public List<ResultSet> getActualResultSets() {
		return actualResultSets;
	}

	public void setActualResultSets(List<ResultSet> actualResultSets) {
		this.actualResultSets = actualResultSets;
	}

}
