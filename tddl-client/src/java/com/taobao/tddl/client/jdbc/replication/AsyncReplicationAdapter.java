package com.taobao.tddl.client.jdbc.replication;

import com.taobao.tddl.common.sync.ReplicationTaskListener;

/*
 * @author guangxia
 * @since 1.0, 2010-4-13 ÏÂÎç05:46:30
 */
public class AsyncReplicationAdapter extends ReplicationAdapter {
	
	private final AsyncRowBasedReplicationListener asyncRowBasedReplicationListener;

	public AsyncReplicationAdapter() {
		super(new AsyncRowBasedReplicationListener());
		asyncRowBasedReplicationListener = (AsyncRowBasedReplicationListener) super.getRowBasedReplicationListener();
	}
	
	public void setInsertSyncLogThreadPoolSize(int threadPoolSize) {
		asyncRowBasedReplicationListener.setInsertSyncLogThreadPoolSize(threadPoolSize);
	}

	public void setReplicationThreadPoolSize(int threadPoolSize) {
		asyncRowBasedReplicationListener.setReplicationThreadPoolSize(threadPoolSize);
	}

	public void setThreadPoolSize(int threadPoolSize) {
		asyncRowBasedReplicationListener.setThreadPoolSize(threadPoolSize);
	}

	public void setWorkQueueSize(int workQueueSize) {
		asyncRowBasedReplicationListener.setWorkQueueSize(workQueueSize);
	}

	public void setInsertSyncLogWorkQueueSize(int insertSyncLogWorkQueueSize) {
		asyncRowBasedReplicationListener.setInsertSyncLogWorkQueueSize(insertSyncLogWorkQueueSize);
	}

	public void setTaskListener(ReplicationTaskListener taskListener) {
		asyncRowBasedReplicationListener.setTaskListener(taskListener);
	}

}
