package com.taobao.tddl.jdbc.druid.exception;

/**
 * @author qihao
 *
 */
public class DruidIllegalException extends Exception {

	private static final long serialVersionUID = -5341803227125385166L;

	public DruidIllegalException() {
		super();
	}

	public DruidIllegalException(String msg) {
		super(msg);
	}

	public DruidIllegalException(Throwable cause) {
		super(cause);
	}

	public DruidIllegalException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
