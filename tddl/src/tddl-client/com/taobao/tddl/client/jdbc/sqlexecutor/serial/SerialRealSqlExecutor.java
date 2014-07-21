package com.taobao.tddl.client.jdbc.sqlexecutor.serial;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.RealSqlContext;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.sqlexecutor.QueryReturn;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutorCommon;
import com.taobao.tddl.client.jdbc.sqlexecutor.UpdateReturn;

/**
 * @author junyu
 * 
 */
public class SerialRealSqlExecutor extends RealSqlExecutorCommon {
	private static final Log logger = LogFactory
			.getLog(SerialRealSqlExecutor.class);

	public SerialRealSqlExecutor(ConnectionManager connectionManager) {
		super(connectionManager);
	}

	public void serialQuery(
			ConcurrentLinkedQueue<QueryReturn> queryReturnQueue,
			ExecutionPlan executionPlan, TStatementImp tStatementImp) {
		setSpecialProperty(tStatementImp, executionPlan);

		boolean isPrepareStatement = this.isPreparedStatement(tStatementImp);

		Map<String, List<RealSqlContext>> sqlMap = executionPlan.getSqlMap();
		for (final Entry<String, List<RealSqlContext>> dbEntry : sqlMap
				.entrySet()) {
			String dbSelectorId = dbEntry.getKey();
			try {
				Connection connection = connectionManager.getConnection(
						dbSelectorId, executionPlan.isGoSlave());

				List<RealSqlContext> sqlList = dbEntry.getValue();

				for (RealSqlContext sql : sqlList) {
					QueryReturn qr = null;
					long start = System.currentTimeMillis();
					if (isPrepareStatement) {
						qr = executeQueryIntervalPST(connection, sql, executionPlan.getSqlMetaData());
					} else {
						qr = executeQueryIntervalST(connection, sql, executionPlan.getSqlMetaData());
					}

					long during = System.currentTimeMillis() - start;

					qr.setCurrentDBIndex(dbSelectorId);
					queryReturnQueue.add(qr);
					profileRealDatabaseAndTables(dbSelectorId, sql, during);
				}
			} catch (SQLException e) {
				//第一时间打印异常
				logger.error(e);
				// 发生异常后，将异常塞入一个QueryReturn里面直接返回，不再进行后续查询
				QueryReturn qr = new QueryReturn();
				qr.add2ExceptionList(e);
				tryCloseConnection(qr.getExceptions(), dbSelectorId);
				queryReturnQueue.add(qr);
				break;
			}
		}
	}

	protected void tryCloseConnection(String dbIndex) {
		tryCloseConnection(null, dbIndex);
	}

	public void serialUpdate(
			ConcurrentLinkedQueue<UpdateReturn> updateReturnQueue,
			ExecutionPlan executionPlan, TStatementImp tStatementImp) {
		setSpecialProperty(tStatementImp, executionPlan);

		boolean isPrepareStatement = this.isPreparedStatement(tStatementImp);

		Map<String, List<RealSqlContext>> sqlMap = executionPlan.getSqlMap();
		for (final Entry<String, List<RealSqlContext>> dbEntry : sqlMap
				.entrySet()) {
			UpdateReturn ur = null;
			if (isPrepareStatement) {
				ur = executeUpdateIntervalPST(executionPlan, dbEntry);
			} else {
				ur = executeUpdateIntervalST(executionPlan, dbEntry);
			}

			updateReturnQueue.add(ur);
		}
	}
}
