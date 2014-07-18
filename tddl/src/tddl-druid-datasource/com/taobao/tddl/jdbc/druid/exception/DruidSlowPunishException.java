package com.taobao.tddl.jdbc.druid.exception;

import java.sql.SQLException;

/**
 * 获取连接时，判断为超时惩罚期，抛出该异常
 * 
 * @author linxuan
 *
 */
public class DruidSlowPunishException extends SQLException {
	private static final long serialVersionUID = 1L;

	public DruidSlowPunishException() {
		super();
	}

	public DruidSlowPunishException(String msg) {
		super(msg);
	}

	public DruidSlowPunishException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public DruidSlowPunishException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

}
