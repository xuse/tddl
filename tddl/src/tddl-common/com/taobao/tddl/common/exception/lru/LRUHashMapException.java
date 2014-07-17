package com.taobao.tddl.common.exception.lru;

public class LRUHashMapException extends Exception {
	public LRUHashMapException()
	{
		super();
	}
	public LRUHashMapException(String message)
	{
		super(message);
	}
	public LRUHashMapException(String message, Throwable cause)
	{
		super(message,cause);
	}
}
