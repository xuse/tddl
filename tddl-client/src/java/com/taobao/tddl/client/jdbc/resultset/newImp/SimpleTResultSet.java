package com.taobao.tddl.client.jdbc.resultset.newImp;

import static com.taobao.tddl.client.util.ExceptionUtils.appendToExceptionList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
 * 重试的逻辑: 1. 写不重试 2. sql 1 对 1 的读取，只要有一个resultSet，就不显示的抛出错误，只打错误log.
 * 如果一个resultSet都么有，则显示抛出错误。 3. sql 1 对 多的读取，只要有一个resultSet，那么就不显示的抛出错误，只打印错误log
 *
 * @author shenxun
 * @author junyu
 *
 */
public class SimpleTResultSet extends ProxyTResultSet {
	private static final Log log = LogFactory.getLog(SimpleTResultSet.class);
	/**
	 * 当前持有的statement
	 */
	protected Statement statement;
	/**
	 * 当前持有的resultSet
	 */
	protected ResultSet resultSet;
	/**
	 * 当前数据库selector 的id
	 */
	protected String currentDBIndex = null;
	protected int fetchSize = -1;
	protected int tableIndex = 0;
	/**
	 * 调用ResultSet的tStatement
	 */
	protected final TStatementImp tStatementImp;

	/**
	 * 执行计划
	 */
	protected final ExecutionPlan executionPlan;

	/**
	 * Sql执行器
	 */
	protected final RealSqlExecutor realSqlExecutor;

	/**
	 * 是否初始化q
	 */
	protected boolean inited = false;

	/**
	 * 到哪结束
	 */
	protected int limitTo = -1;
	/**
	 * 从哪开始
	 */
	protected int limitFrom = 0;


	private final long startQueryTime;

	private boolean hasMoreResourcesOnInit = false;

	public SimpleTResultSet(TStatementImp tStatementImp, ConnectionManager connectionManager,
			ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor) throws SQLException {
		this(tStatementImp, connectionManager, executionPlan,realSqlExecutor,true);
	}

	public SimpleTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor,boolean init)
			throws SQLException {
		super(connectionManager);
		this.tStatementImp = tStatementImp;

		// add by jiechen.qzm 关闭结果集时的判断需要这个时间
		super.setResultSetProperty(tStatementImp);
		// add end

		this.executionPlan = executionPlan;
		this.realSqlExecutor=realSqlExecutor;
		startQueryTime = System.currentTimeMillis();
		if(init){
			//初始化一下
			hasMoreResourcesOnInit = superReload();
		}
	}
	public boolean next() throws SQLException {
		checkClosed();

		if (limitTo == 0) {
			return false;
		}
		// 初始化
		if (!inited) {
			inited = true;
			if(!hasMoreResourcesOnInit){
				//表示没有可供选择的数据源，规则内数据源为空，或异常，总而言之正常情况下不可能走到这里。
				throw new SQLException("结果集为空，可能是由于有空库空表或query异常导致，不应该走到这里");
			}
			Map<String, List<RealSqlContext>> map = executionPlan.getSqlMap();
			if (map.size() == 0) {
				throw new SQLException("should not be here");
			}
			int tableSize = map.values().iterator().next().size();
			if (tableSize != 1 || map.size() != 1) {
				for (int i = 0; i < limitFrom; i++) {
					if (!next()) {
						// 如果next返回false,则表示当前已经没有数据可以返回，直接返回false
						return false;
					}
				}
			}

		}
		/*
		 * // 表示当前已经没有可用的resultSet了。 if (resultSet == null) { return false; }
		 */
		while (true) {
			if (resultSet == null) {
				return false;
			}

			// exception throw by real resultSet , we can do nothing just throw
			// it.
			
			if (resultSet.next()) {
				// 有可用资源，那么指针已经下移，返回true即可
				limitTo--;
				return true;
			}
			
			if (!superReload()) {
				// 如果没有可用资源了，要返回false。如果还有可用资源，那么reload会重置 statement和resultset
				// 走到resultSet.next()继续判断是否有可用资源
				return false;
			}

		}

	}

	protected boolean superReload() throws SQLException{
		List<SQLException> sqlExceptions = new LinkedList<SQLException>();
		/**
		 * 无论如何先清掉之前的statement和resultset
		 * 因为SimpleTResultSet只持有一个resultset和statement
		 * 所以无论串行和并行必须在reload之前清掉之前
		 * 持有的statement和resultset
		 */
		closeAndClearResources(sqlExceptions);

		QueryReturn qr=realSqlExecutor.query();
		
		//如果qr为null,说明结果集取尽
		if(null==qr){
			return false;
		}

		if(null==qr.getExceptions()){
			this.statement=qr.getStatement();
			this.resultSet=qr.getResultset();
			this.currentDBIndex=qr.getCurrentDBIndex();
			super.currentResultSet=resultSet;
			return true;
		}else{
			sqlExceptions=appendToExceptionList(sqlExceptions,qr.getExceptions());
			writeLogOrThrowSQLException("TDDL print sqlException while retry :",
					sqlExceptions);

			return false;
		}
	}

	public int getFetchDirection() throws SQLException {
		log.debug("invoke getFetchDirection");
		checkClosed();
		return FETCH_FORWARD;
	}

	public void setFetchDirection(int direction) throws SQLException {
		log.debug("invoke setFetchDirection");
		checkClosed();
		if (direction != FETCH_FORWARD) {
			throw new SQLException("only support fetch direction FETCH_FORWARD");
		}
	}

	/**
	 * 清掉并关闭当前statement的资源
	 *
	 * @param exceptions
	 * @param closeConnection
	 * @throws SQLException
	 */
	protected List<SQLException> closeAndClearResources(
			List<SQLException> exceptions) {
		if (resultSet != null) { 
			try {
				resultSet.close();
			} catch (SQLException e) {
				exceptions = appendToExceptionList(exceptions, e);
			} finally {
				resultSet = null;
			}
		}
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				exceptions = appendToExceptionList(exceptions, e);
			} finally {
				statement = null;
			}
		}
		// 打扫战场的时候不抛出异常，如果抛出异常，则肯定最少查了一次了，所以不会走到catch段中
		return exceptions;
	}

	protected void checkClosed() throws SQLException {
		if (closed) {
			throw new SQLException(
					"No operations allowed after result set closed.");
		}
	}

	protected void checkPoint() throws SQLException {
		if (resultSet == null) {
			throw new SQLException("结果集为空或已经取尽");
		}
	}

	public void setFetchSize(int rows) throws SQLException {
		if (log.isDebugEnabled()) {
			log.debug("invoke setFetchSize");
		}

		checkClosed();

		if (rows < 0) {
			throw new SQLException("fetch size must greater than or equal 0");
		}

		this.fetchSize = rows;
	}

	// TODO: not used
	public int getFetchSize() throws SQLException {
		if (log.isDebugEnabled()) {
			log.debug("invoke getFetchSize");
		}

		checkClosed();

		return fetchSize;
	}

	public int getLimitTo() {
		return limitTo;
	}

	public void setLimitTo(int limitTo) {
		this.limitTo = limitTo;
	}

	public int getLimitFrom() {
		return limitFrom;
	}

	public void setLimitFrom(int limitFrom) {
		this.limitFrom = limitFrom;
	}

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

		closed = true;
		// 统计整个查询的耗时。或许不是很准，但比较重要。
		long elapsedTime = System.currentTimeMillis() - startQueryTime;
		profileDuringTime(exceptions, executionPlan.getVirtualTableName().toString(),
				executionPlan.getOriginalSql(), elapsedTime);
		try {
			if (resultSet != null) {
				resultSet.close();
			}
		} catch (SQLException e) {
			exceptions = appendToExceptionList(exceptions, e);
		} finally {
			resultSet = null;
		}

		try {
			if (this.statement != null) {
				this.statement.close();
			}
		} catch (SQLException e) {
			exceptions = appendToExceptionList(exceptions, e);
		} finally {
			this.statement = null;
		}

		// 最后要尝试关闭当前连接
		if(currentDBIndex != null){
			//currentDBIndex == null则表示还没有初始化就调用了关闭
			exceptions = tryCloseConnection(exceptions, currentDBIndex);
		}

		//以防万一
		for (String key : executionPlan.getSqlMap().keySet()) {
			exceptions = tryCloseConnection(exceptions, key);
		}

		writeLogOrThrowSQLException("sql exception during close resources",
				exceptions);
	}

	private void writeLogOrThrowSQLException(String message,
			List<SQLException> sqlExceptions) throws SQLException {
		// 这时候抛出异常,如果有异常的话
		ExceptionUtils.throwSQLException(sqlExceptions, executionPlan
				.getOriginalSql(), executionPlan.getOriginalArgs());

	}

	public Statement getStatement() throws SQLException {
		return tStatementImp;
	}
}
