package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;

/*
 * @author guangxia
 * @since 1.0, 2009-9-18 下午05:44:13
 */
public class MinTResultSet extends MaxMinTResultSet {

	public MinTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor);
	}
	public MinTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor,boolean init)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor,init);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ResultSet reducer() throws SQLException {
		ResultSet minResultSet = actualResultSets.get(0);
		minResultSet.next();
		Comparable<Object> min = (Comparable<Object>) minResultSet.getObject(1);

		for (int i = 1; i < actualResultSets.size(); i++) {
			ResultSet resultSet = actualResultSets.get(i);
			resultSet.next();
			Comparable<Object> comp = (Comparable<Object>) resultSet
					.getObject(1);
			if (min == null || comp == null) {
				if (comp != null) {
					minResultSet = resultSet;
					min = comp;
				}
			} else if (min.compareTo(comp) > 0) {
				minResultSet = resultSet;
				min = comp;
			}
		}
		return minResultSet;
	}
}
