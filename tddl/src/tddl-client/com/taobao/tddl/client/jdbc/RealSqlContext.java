package com.taobao.tddl.client.jdbc;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;

/**
 * 一个执行在某个数据库上的sql+参数的总和
 * 
 * @author shenxun
 *
 */
public interface RealSqlContext {
	/**
	 * 获取当前sql.非空 ，必填。
	 * @return
	 */
	public String getSql();
	
	/**
	 * 获取当前sql执行的表名，主要用于记录log.
	 * 
	 * 非空，如果不能获得，则返回 ""
	 * @return
	 */
	public String getRealTable();
	
	/**
	 * 获取与当前执行sql配套的参数。可为空，如果为空表示当前sql没有参数。
	 * 
	 * @return
	 */
	public Map<Integer, ParameterContext>  getArgument();
}
