package com.taobao.tddl.common.sync;

import java.sql.SQLException;

public class SyncUtils {
	public static int getSyncLogTableSuffix(String syncLogId) {
		return Integer.valueOf(syncLogId.substring(0, 1), 16) / 2;
	}
	
	public static class SQLExceptionInfo {
		public int getErrorCode() {
			return errorCode;
		}
		public void setErrorCode(int errorCode) {
			this.errorCode = errorCode;
		}
		private int errorCode;
		public String getSQLState() {
			return SQLState;
		}
		public void setSQLState(String state) {
			SQLState = state;
		}
		private String SQLState;
	}

	public static SQLExceptionInfo getSqlState(SQLException ex) {
		SQLExceptionInfo expInfo = new SQLExceptionInfo();
		String sqlState = ex.getSQLState();
		if (sqlState == null) {
			ex = ex.getNextException();
		} 
		if(ex != null){
			expInfo.setSQLState(ex.getSQLState());
			expInfo.setErrorCode(ex.getErrorCode());
		}
		return expInfo;
	}
}
