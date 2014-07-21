package com.taobao.tddl.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 用户管理一对多的链接关系。
 * 
 * 主要是处理事务的问题，Manager会根据引用计数决定当前连接是否应该被关闭掉。
 * 
 * 在没有事务的时候，主要的模型是谁请求谁关闭。
 * 
 * 在有事务的时候，模型变为事务开启后不再响应关闭请求，只去掉引用
 * 
 * @author shenxun
 *
 */
public interface ConnectionManager {
	/**
	 * 是否是自动提交
	 * 
	 * @return
	 */
//	此方法被Connection要求，Manager说明一下autoCommit时的行为
	public boolean getAutoCommit() throws SQLException;

//	此方法被Connection要求，Manager说明一下autoCommit时的行为void setAutoCommit(boolean isAutoCommit) throws SQLException;
	
	/**
	 * 尝试获取一个连接
	 * 公共: 
	 * 
	 * 事务中:
	 * 
	 * 如果goMaster == true ，数据源超过一个，
	 * 		会抛错误。
	 * 
	 * 如果在非事务。
	 * 		不会抛错误。
	 * 
	 * 在每次尝试从新获取连接的时候，都必须显示的将连接设置RetryableDatasourceGrooup.autocommit()
	 * 为指定的值。
	 * 
	 * 存疑的地方是，如果在事务中，查询其他库，是否应该允许他查询呢？
	 * 
	 * @param dbIndex
	 * @return
	 */
	Connection getConnection(String dbIndex,boolean goSlave) throws SQLException;

	/**
	 * 请求关闭当前连接
	 * 
	 * 非事务状态，如果有且仅有一个TStatement被创建，则关闭请求链接。如果有多个TStatement被创建，则什么都不做.
	 * 这里有个前提条件，也就是一个TStatement只可能对应一个TResultSet.因此当TStatement和其对应的TResultSet，只有可能有一个对象
	 * 调用tryClose方法。
	 * 
	 * 事务状态，不关闭连接。
	 * 
	 * 【放弃了最开始的引用计数的想法，因为模型相对复杂，放弃带来的负面影响是：
	 * 		在非事务状态下，多于一个TStatement的时候会关闭不及时
	 * 】
	 * 
	 * 调用此方法的TStatement和TResultSet ，获取这个异常，貌似也没什么好办法，log一下
	 * 			
	 * @param dbIndex
	 * @throws SQLException 如果关闭时发生了异常会直接抛出 需要被显示的cache住
	 */
	void tryClose(String dbIndex) throws SQLException;
	
	/**
	 * 获取代理的连接
	 * 
	 * @return
	 */
	Connection getProxyConnection();
	
	/**
	 * 移除当前statement
	 * 
	 * @param statement
	 */
	void removeCurrentStatement(Statement statement);
	
	boolean containDBIndex(String dbIndex);
}
