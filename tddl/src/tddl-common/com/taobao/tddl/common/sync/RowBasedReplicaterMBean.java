package com.taobao.tddl.common.sync;


/**
 * RowBasedReplicater JMX MBean接口
 * 
 * @author linxuan
 */
public interface RowBasedReplicaterMBean {
	/**
	 * 三个队列的大小
	 */
	int getReplicationQueueSize();

	int getDeleteSyncLogQueueSize();

	int getUpdateSyncLogQueueSize();


	/**
	 * 三个线程池的完成任务数
	 */
	long getCompletedReplicationCount();

	/**
	 * 删除日志线程池完成任务数，每个任务都是批量删除的 
	 */
	long getCompletedDeleteSyncLogCount();

	/**
	 * 更新日志线程池完成任务数，每个任务都是批量更新的 
	 */
	long getCompletedUpdateSyncLogCount();

	
	/**
	 * 动态监控和调整bucketSize
	 */
	int getDeleteBatchSize();

	void setDeleteBatchSize(int bucketSize);

	int getUpdateBatchSize();

	void setUpdateBatchSize(int bucketSize);
}
