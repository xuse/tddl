package com.taobao.tddl.jdbc.druid.exception;

import java.sql.SQLException;

/**
 * Atom层通过ExceptionSorter检测到数据源不可用时抛出，
 * 或者数据库不可用，同时没有trylock到重试机会时也抛出
 * 便于group层重试
 * 
 * 
 * @author linxuan
 *
 */
public class DruidNotAvailableException extends SQLException {
	private static final long serialVersionUID = 1L;

	public DruidNotAvailableException() {
		super();
	}

	public DruidNotAvailableException(String msg) {
		super(msg);
	}

	public DruidNotAvailableException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public DruidNotAvailableException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

}
