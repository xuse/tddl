package com.taobao.tddl.client.jdbc.replication;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.sqlexceptionwrapper.TDDLSQLExceptionWrapper;

/**
 * 插入日志库失败的标志Exception
 * 
 * @author linxuan
 * 
 */
public class SaveSyncLogFailedException extends TDDLSQLExceptionWrapper {
	private static final long serialVersionUID = 1L;

	public SaveSyncLogFailedException(String message,
			SQLException targetSQLESqlException) {
		super(message, targetSQLESqlException);
	}
}
