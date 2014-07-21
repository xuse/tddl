package com.taobao.tddl.client.jdbc.resultset.newImp;

import static com.taobao.tddl.client.util.ExceptionUtils.appendToExceptionList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.RealSqlContext;
import com.taobao.tddl.client.jdbc.TPreparedStatementImp;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.util.ExceptionUtils;
import com.taobao.tddl.common.channel.SqlMetaData;
import com.taobao.tddl.common.util.SqlMetaDataUtil;

/**
 * 一个最最精简的结果集包装类，提供浅包装。
 *
 * 所有方法直接指向resultSet的方法，只适用于查一张表的场景。
 *
 * @author shenxun
 * @author junyu
 *
 */
public class ShallowTResultSetWrapper extends ProxyTResultSet implements ResultSet{
	private long startQueryTime = 0;

	private final boolean isPreparedStatement;
	/**
	 * 真正的statement
	 */
	private  Statement statement;
	/**
	 * 真正的resultSet
	 */
	private  ResultSet resultSet;
	private TStatementImp tStatementImp;
	private ExecutionPlan executionPlan;
	public ShallowTResultSetWrapper(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan) throws SQLException {
		super(connectionManager);
		startQueryTime = System.currentTimeMillis();
		Map<String/* db Selector id */, List<RealSqlContext>/* 真正在当前database上执行的sql的列表 */> sqlMap = executionPlan
				.getSqlMap();
		if (tStatementImp instanceof TPreparedStatementImp) {
			isPreparedStatement = true;
		} else {
			isPreparedStatement = false;
		}
		this.tStatementImp = tStatementImp;
		this.executionPlan = executionPlan;

		//bug 2011-10-20 junyu,not add before;
		// modified by jiechen.qzm 2012-02-23
		super.setResultSetProperty(tStatementImp);
		// modified end

		boolean firstElement = true;
		//check size
		for (Entry<String, List<RealSqlContext>> dbEntry : sqlMap.entrySet()) {
			String dbSelectorId = dbEntry.getKey();

				Connection connection = connectionManager
						.getConnection(dbSelectorId, executionPlan.isGoSlave());
				List<RealSqlContext> sqlList = dbEntry.getValue();
				for (RealSqlContext sql : sqlList) {
					long start = System.currentTimeMillis();

					if (!isPreparedStatement) {
						executeQueryIntervalST(connection, sql,tStatementImp, executionPlan.getSqlMetaData());
					} else {
						executeQueryIntervalPST(connection, sql,tStatementImp, executionPlan.getSqlMetaData());
					}
					// 如果能够走到这里，表示数据库无异常，可以终止重试循环。

					long during = System.currentTimeMillis() - start;
					profileRealDatabaseAndTables(dbSelectorId, sql, during);
					if(firstElement){
						firstElement = false;
					}else{
						throw new SQLException("only one table execution was allowed on ShallowTRS! ");
					}
				}
		}
	}

	/*private void setResultSetProperty(TStatementImp tStatementImp) throws SQLException{
		setResultSetType(tStatementImp.getResultSetType());
		setResultSetConcurrency(tStatementImp.getResultSetConcurrency());
		setResultSetHoldability(tStatementImp.getResultSetHoldability());
		setFetchSize(tStatementImp.getFetchSize());
		setMaxRows(tStatementImp.getMaxRows());
		setQueryTimeout(tStatementImp.getQueryTimeout());
	}*/


	private void executeQueryIntervalST(Connection connection,
			RealSqlContext sql,TStatementImp statementImp, SqlMetaData sqlMetaData) throws SQLException {
		// 建立会话
		statement = createStatementInternal(connection);
		statement.setQueryTimeout(getQueryTimeout());
		statement.setFetchSize(getFetchSize());
		statement.setMaxRows(getMaxRows());
		SqlMetaDataUtil.fillStatement(statement, sqlMetaData);
		resultSet = statement.executeQuery(sql.getSql());
	    /**
	     * add by junyu
	     */
		super.currentResultSet=resultSet;

	}

	private void executeQueryIntervalPST(Connection connection,
			RealSqlContext sql,TStatementImp statementImp, SqlMetaData sqlMetaData) throws SQLException {
		// 建立会话
		PreparedStatement stmt = prepareStatementInternal(connection, sql
				.getSql());
		stmt.setQueryTimeout(getQueryTimeout());
		stmt.setFetchSize(getFetchSize());
		stmt.setMaxRows(getMaxRows());

		setParameters(stmt, sql.getArgument());
		statement = stmt;
		SqlMetaDataUtil.fillStatement(statement, sqlMetaData);
		resultSet = stmt.executeQuery();
		/**
		 * add by junyu
		 */
		super.currentResultSet=resultSet;

	}
	public void checkSize(Map<String/* db Selector id */, List<RealSqlContext>/* 真正在当前database上执行的sql的列表 */> sqlMap)
	throws SQLException{
		if(sqlMap.size() != 1){
			throw new SQLException("should not be here , ONLY ONE ds allowed!");
		}
	}

	public void checkRSIsNull() throws SQLException{
		if(resultSet == null){
			throw new SQLException("exception on execution query,result set is already closed!");
		}
	}

	/**
	 * bug fix by shenxun : 原来会发生一个情况就是如果TStatement调用了close()方法
	 * 而本身其管理的TResultSet没有closed时候。外部会使用iterator来遍历每一个
	 * TResultSet，调用关闭的方法，但因为TResultSet的close方法会回调
	 * TStatement里面用于创建iterator的Set<ResultSet>对象，并使用remove方法。
	 * 这就会抛出一个concurrentModificationException。
	 *
	 * @param removeThis
	 * @throws SQLException
	 */
	public void closeInternal() throws SQLException {
		checkRSIsNull();

		List<SQLException> exceptions = null;


		if (closed) {
			return;
		}
		// 统计整个查询的耗时。或许不是很准，但比较重要。
		long elapsedTime = System.currentTimeMillis() - startQueryTime;

		profileDuringTime(exceptions, executionPlan.getVirtualTableName().toString(),
				executionPlan.getOriginalSql(), elapsedTime);

		try {
			// 关闭resultset
				try {
					resultSet.close();
				} catch (SQLException e) {
					exceptions = appendToExceptionList(exceptions, e);
				}

			// 关闭statement
				try {
					statement.close();
				} catch (SQLException e) {
					exceptions = appendToExceptionList(exceptions, e);
				}
		} finally {
			//无意义
			closed = true;
		}
		
		//最简单的result，对应的查询计划是一条sql，连接在connection.close()方法中关闭，这里不做提前关闭
		//exceptions = closeConnection(exceptions);
		
		//抛出异常，如果exception 不为null
		ExceptionUtils.throwSQLException(exceptions,
				"sql exception during close resources", Collections.emptyList());
	}
	
	private List<SQLException> closeConnection(List<SQLException> exceptions) {
		// 通知父类关闭所有连接
		for (String key : executionPlan.getSqlMap().keySet()) {
			exceptions = tryCloseConnection(exceptions, key);
		}
		return exceptions;
	}

	public Statement getStatement() throws SQLException {
		checkRSIsNull();
		//shenxun : 这里返回包装类
		return tStatementImp;
	}
}
