package com.taobao.tddl.common.exception;

public class MissingConfigException extends RuntimeException{
	
	private static final long serialVersionUID = 5761363021328302754L;

	public MissingConfigException() {
		super();
	}

	public MissingConfigException(String msg) {
		super(msg);
	}

	public MissingConfigException(Throwable cause) {
		super(cause);
	}

	public MissingConfigException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
