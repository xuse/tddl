package com.taobao.tddl.jdbc.druid.exception;

public class DruidAlreadyInitException extends Exception {

	private static final long serialVersionUID = -3907211238952987907L;

	public DruidAlreadyInitException() {
		super();
	}

	public DruidAlreadyInitException(String msg) {
		super(msg);
	}

	public DruidAlreadyInitException(Throwable cause) {
		super(cause);
	}

	public DruidAlreadyInitException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
