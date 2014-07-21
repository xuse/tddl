//Copyright(c) Taobao.com
package com.taobao.tddl.rule.le.exception;
/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-5-10上午10:30:21
 */
public class ResultCompareDiffException extends Exception {
    /**
	 * 
	 */
	private static final long serialVersionUID = -1466037353355999132L;

	public ResultCompareDiffException(){
    	super();
    }
    
    public ResultCompareDiffException(String message){
    	super(message);
    }
    
    public ResultCompareDiffException(Throwable cause){
    	super(cause);
    }
    
    public ResultCompareDiffException(String message,Throwable cause){
    	super(message,cause);
    }
}
