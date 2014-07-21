package com.taobao.tddl.jdbc.atom.exception;

import java.sql.SQLException;

/**
 * 获取连接时，判断为超时惩罚期，抛出该异常
 * 
 * @author linxuan
 *
 */
public class AtomSlowPunishException extends SQLException {
	private static final long serialVersionUID = 1L;

	public AtomSlowPunishException() {
		super();
	}

	public AtomSlowPunishException(String msg) {
		super(msg);
	}

	public AtomSlowPunishException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public AtomSlowPunishException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

}
