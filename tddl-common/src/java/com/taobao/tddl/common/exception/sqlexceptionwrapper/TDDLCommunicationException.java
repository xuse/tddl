package com.taobao.tddl.common.exception.sqlexceptionwrapper;

import java.sql.SQLException;

public class TDDLCommunicationException extends TDDLSQLExceptionWrapper{

	public TDDLCommunicationException(String message,
			SQLException targetSQLESqlException) {
		super(message, targetSQLESqlException);
	}

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -3502922457609932248L;

}
