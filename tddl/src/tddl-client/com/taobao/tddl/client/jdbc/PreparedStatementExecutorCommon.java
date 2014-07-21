package com.taobao.tddl.client.jdbc;

import static com.taobao.tddl.client.util.ExceptionUtils.appendToExceptionList;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.client.jdbc.resultset.newImp.DummyTResultSet;
import com.taobao.tddl.client.jdbc.resultset.newImp.EmptySimpleTResultSet;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;

public abstract class PreparedStatementExecutorCommon extends
		ExecuteSQLProfiler {
	public PreparedStatementExecutorCommon(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}
	
	protected void setParameters(PreparedStatement ps,
			Map<Integer, ParameterContext> parameterSettings)
			throws SQLException {
		ParameterMethod.setParameters(ps, parameterSettings);
	}

	/**
	 * 连接管理器
	 */
	protected final ConnectionManager connectionManager;

	protected List<SQLException> tryCloseConnection(
			List<SQLException> exceptions, String dbSelectorId) {
		try {
			connectionManager.tryClose(dbSelectorId);
		} catch (SQLException e) {
			exceptions = appendToExceptionList(exceptions, e);
		}
		return exceptions;
	}

	protected final DummyTResultSet getEmptyResultSet(
			TStatementImp tStatementImp) throws SQLException {
		return new EmptySimpleTResultSet(tStatementImp, connectionManager,
				null, null);
	}
}
