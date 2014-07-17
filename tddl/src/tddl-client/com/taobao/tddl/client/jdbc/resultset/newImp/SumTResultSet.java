package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.resultset.helper.AddRealizer;
import com.taobao.tddl.client.jdbc.resultset.helper.AddRealizer.Add;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;

/**
 * @author guangxia
 * @author junyu modify the Add class
 */
public class SumTResultSet extends OnceNextTResultSet {
	public SumTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor);
	}

	public SumTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor,
			List<ResultSet> testResultSet, Set<Statement> testStatement)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor,testResultSet,
				testStatement);
	}

	protected ResultSet reducer() throws SQLException {
		ResultSet resultSet;
		Object sum = null;
		Add<Object> add = null;

		for (int i = 0; i < actualResultSets.size(); i++) {
			resultSet = actualResultSets.get(i);
			resultSet.next();
			Object data = resultSet.getObject(1);
			if (data != null) {
				if (sum == null) {
					sum = data;
					add = AddRealizer.getNumberAdd(sum);
					if (null == add) {
						throw new SQLException(
								"The group function 'SUM' does not supported the type '"
										+ sum.getClass() + "'");
					}
				} else {
					sum = add.add(sum, resultSet.getObject(1));
				}
			}
		}

		super.value = sum;
		super.isNull = sum == null;
		return null;
	}
}
