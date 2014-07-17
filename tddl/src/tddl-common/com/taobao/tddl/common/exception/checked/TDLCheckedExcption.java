package com.taobao.tddl.common.exception.checked;

public class TDLCheckedExcption extends Exception{
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -1186363001286203116L;
	public TDLCheckedExcption() {
		super();
	}
	public TDLCheckedExcption(Throwable throwable){
		super(throwable);
	}
    public TDLCheckedExcption(String message, Throwable cause) {
        super(message, cause);
    }
	public TDLCheckedExcption(String arg) {
		super(arg);
	}
}
