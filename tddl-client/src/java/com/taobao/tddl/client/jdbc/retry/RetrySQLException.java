//package com.taobao.tddl.client.jdbc.retry;
//
//import java.sql.SQLException;
//
//import javax.sql.DataSource;
//
//public class RetrySQLException extends RuntimeException {
//	/**
//	 * serialVersionUID
//	 */
//	private static final long serialVersionUID = -1060762647387139521L;
//	final SQLException sqlException;
//	final DataSource currentDataSource;
//	public RetrySQLException(SQLException sqlException,DataSource currentDataSource) {
//		this.sqlException = sqlException;
//		this.currentDataSource = currentDataSource;
//	}
//	
//	
//	public DataSource getCurrentDataSource() {
//		return currentDataSource;
//	}
//
//
//	public SQLException getSqlException() {
//		return sqlException;
//	}
//	
//}
