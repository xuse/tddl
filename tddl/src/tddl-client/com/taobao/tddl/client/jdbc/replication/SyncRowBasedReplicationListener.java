package com.taobao.tddl.client.jdbc.replication;

import com.taobao.tddl.client.jdbc.SqlExecuteEvent;
import com.taobao.tddl.common.sync.RowBasedReplicationContext;
import com.taobao.tddl.common.sync.RowBasedReplicationExecutor;

public class SyncRowBasedReplicationListener extends RowBasedReplicationListener {
	protected void doAfterSqlExecute(RowBasedReplicationContext context) {
		context.setReplicationStartTime(System.currentTimeMillis());
		RowBasedReplicationExecutor.execute(context, true);
	}

	@Override
	protected void asyncInsertSyncLog2Db(SqlExecuteEvent event) {
		throw new UnsupportedOperationException("asyncInsertSyncLog2Db:同步Listener不支持异步插入日志库");
	}
}
