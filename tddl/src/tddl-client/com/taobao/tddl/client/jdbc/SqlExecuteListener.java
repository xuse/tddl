package com.taobao.tddl.client.jdbc;

import java.sql.SQLException;

import com.taobao.tddl.client.jdbc.listener.Context;

public interface SqlExecuteListener {
	/**
	 * 用来处理统一配置的传递
	 */
	void init(TDataSource tDataSource);
	void beforeSqlExecute(Context context) throws SQLException;
	void afterSqlExecute(Context context) throws SQLException;
//	void afterTxRollback(Context context) throws SQLException;
}
