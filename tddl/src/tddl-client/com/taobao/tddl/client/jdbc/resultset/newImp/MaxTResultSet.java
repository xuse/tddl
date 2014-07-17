package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;

/*
 * @author guangxia
 * @since 1.0, 2009-9-18 ÏÂÎç05:37:57
 */
public class MaxTResultSet extends MaxMinTResultSet {

	public MaxTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor);
	}
	public MaxTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor,boolean init)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor,init);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ResultSet reducer() throws SQLException {
		ResultSet maxResultSet = actualResultSets.get(0);
		maxResultSet.next();
		Comparable<Object> max = (Comparable<Object>) maxResultSet.getObject(1);

		for (int i = 1; i < actualResultSets.size(); i++) {
			ResultSet resultSet = actualResultSets.get(i);
			resultSet.next();
			Comparable<Object> comp = (Comparable<Object>) resultSet
					.getObject(1);
			if (max == null || comp == null) {
				if (comp != null) {
					maxResultSet = resultSet;
					max = comp;
				}
			} else if (max.compareTo(comp) < 0) {
				maxResultSet = resultSet;
				max = comp;
			}
		}
		return maxResultSet;
	}

}
