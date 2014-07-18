package com.taobao.tddl.jdbc.druid.exception;

/**
 * @author qihao
 *
 */
public class DruidInitialException extends Exception {

	private static final long serialVersionUID = -2933446568649742125L;

	public DruidInitialException() {
		super();
	}

	public DruidInitialException(String msg) {
		super(msg);
	}

	public DruidInitialException(Throwable cause) {
		super(cause);
	}

	public DruidInitialException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
