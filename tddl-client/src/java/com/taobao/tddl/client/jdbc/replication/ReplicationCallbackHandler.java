package com.taobao.tddl.client.jdbc.replication;

import java.sql.SQLException;
import java.util.List;

import com.taobao.tddl.client.jdbc.SqlExecuteEvent;

/**
 * 复制过程各关键事件的回调处理器
 * 
 * @author linxuan
 *
 */
public interface ReplicationCallbackHandler {
	/**
	 * 插入日志库失败时回调
	 * @param event
	 * @param exceptions
	 */
	void insertSyncLogFailed(SqlExecuteEvent event, List<SQLException> exceptions) throws SQLException;
	
	//...其他事件
}
