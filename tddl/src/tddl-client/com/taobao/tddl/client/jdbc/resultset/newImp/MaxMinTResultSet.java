package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;

public abstract class MaxMinTResultSet extends PlainAbstractTResultSet {
	private int cursor;

	public MaxMinTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor);
	}
	public MaxMinTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor,boolean init)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor,init);
	}

	@Override
	public boolean next() throws SQLException {
		checkClosed();
		if (cursor > 0) {
			return false;
		}
		super.currentResultSet=reducer();
		cursor++;
		return true;
	}

	protected abstract ResultSet reducer() throws SQLException;
	
	@Override
	protected void checkRSIsClosedOrNull() throws SQLException{
		if(closed){
			throw new SQLException("No operations allowed after result set closed.");
		}
		
		if(super.currentResultSet == null){
			throw new SQLException("currentresultset is null or getXXX() is not surported!");
		}
		
		if (cursor != 1) {
			throw new SQLException("cursor should not be "+cursor);
		}
	}
}
