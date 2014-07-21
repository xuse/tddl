package com.taobao.tddl.client.jdbc;

import java.sql.SQLException;

import com.taobao.tddl.client.pipeline.PipelineFactory;

public class AllowAllLevelTconnection extends AllowReadLevelTConnection {
	public AllowAllLevelTconnection(String appName, boolean enableProfileRealDBAndTables,PipelineFactory pipelineFactory) {
		super(appName, enableProfileRealDBAndTables,pipelineFactory);
	}

	protected boolean validThrowSQLException(String dbIndex, boolean isGoSlave)
			throws SQLException {
		if (transactionKey == null) {
			// 如果还没有指定过transactionkey,那么当前的新连接就作为默认的transaction连接存在
			transactionKey = dbIndex;
			return true;
		} else {
			//允许写入到多个数据节点中，所以这里不抛出异常。
			return false;
		}
	}
}
