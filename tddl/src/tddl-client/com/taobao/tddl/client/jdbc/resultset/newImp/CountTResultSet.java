package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;

/**
 * @author guangxia
 * @author junyu
 * 
 */
public class CountTResultSet extends OnceNextTResultSet {
	public CountTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,
			RealSqlExecutor realSqlExecutor) throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor);
	}

	public CountTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor,
			List<ResultSet> testResultSet, Set<Statement> testStatement)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan, realSqlExecutor,testResultSet,
				testStatement);
	}

	protected ResultSet reducer() throws SQLException {
		long sum = 0;
		for (ResultSet resultSet : actualResultSets) {
			resultSet.next();
			sum += resultSet.getLong(1);
		}
		super.value = sum;
		super.isNull = false;
		return null;
	}
}
