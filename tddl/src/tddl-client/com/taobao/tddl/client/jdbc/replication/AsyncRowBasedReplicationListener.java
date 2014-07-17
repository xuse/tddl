package com.taobao.tddl.client.jdbc.replication;

import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.jdbc.SqlExecuteEvent;
import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.client.jdbc.replication.ReplicationSwitcher.ReplicationConfigAware;
import com.taobao.tddl.client.util.LogUtils;
import com.taobao.tddl.common.sync.ReplicationTaskListener;
import com.taobao.tddl.common.sync.RowBasedReplicationContext;
import com.taobao.tddl.common.sync.RowBasedReplicationTask;

public class AsyncRowBasedReplicationListener extends RowBasedReplicationListener implements ReplicationConfigAware {
	private static final Log logger = LogFactory.getLog(AsyncRowBasedReplicationListener.class);
	private static final Log localFailSyncLog = LogFactory.getLog(LogUtils.TDDL_LOCAL_FAIL_SYNC_LOG);

	private int threadPoolSize = 4;
	private int workQueueSize = 4096;
	private ThreadPoolExecutor replicationExecutor;

	private int insertSyncLogThreadPoolSize = 2;
	private int insertSyncLogWorkQueueSize = 1024;
	private ThreadPoolExecutor insertSyncLogExecutor;

	private ReplicationTaskListener taskListener;

	public void init(TDataSource tds) {
		super.init(tds);
		init();
		tds.state.setReplicationExecutor(replicationExecutor);
		tds.state.setReplicationQueueSize(workQueueSize);
	}
	
	public void init() {
		super.init();
		super.replicationSwitcher.addReplicationConfigAware(this);

		replicationExecutor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<Runnable>(workQueueSize), new RejectedExecutionHandler() {
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						logger.warn("A RowBasedReplicationTask discarded");
					}
				});

		insertSyncLogExecutor = new ThreadPoolExecutor(insertSyncLogThreadPoolSize, insertSyncLogThreadPoolSize, 0L,
				TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(insertSyncLogWorkQueueSize),
				new RejectedExecutionHandler() {
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						AsyncRowBasedReplicationListener.super.insertSyncLog2LocalFile(localFailSyncLog,
								((InsertSyncLogTask) r).event);
					}
				});
	}

	static class InsertSyncLogTask implements Runnable {
		private final SqlExecuteEvent event;
		private final RowBasedReplicationListener listener;

		public InsertSyncLogTask(SqlExecuteEvent event, RowBasedReplicationListener listener) {
			this.event = event;
			this.listener = listener;
		}

		public void run() {
			try {
				//尝试插入日志库
				listener.insertSyncLog2Db(event);
			} catch (SQLException e) {
				//失败时插入单独的log
				RowBasedReplicationListener.insertSyncLog2LocalFile(localFailSyncLog, event);
			}
		}
	}

	protected void doAfterSqlExecute(RowBasedReplicationContext context) {
		replicationExecutor.execute(new RowBasedReplicationTask(context, taskListener));
	}

	@Override
	protected void asyncInsertSyncLog2Db(final SqlExecuteEvent event) {
		//先同步插入一条全量log
		RowBasedReplicationListener.insertSyncLog2LocalFile(event);
		insertSyncLogExecutor.execute(new InsertSyncLogTask(event, this));
	}

	public void setInsertSyncLogThreadPoolSize(int threadPoolSize) {
		StringBuilder sb = new StringBuilder("InsertSyncLogThreadPoolSize [").append(this.insertSyncLogThreadPoolSize);
		sb.append("] switching to [").append(threadPoolSize).append("] ");

		this.insertSyncLogThreadPoolSize = threadPoolSize;
		this.insertSyncLogExecutor.setCorePoolSize(this.insertSyncLogThreadPoolSize);
		this.insertSyncLogExecutor.setMaximumPoolSize(this.insertSyncLogThreadPoolSize);

		sb.append("succeed.");
		logger.warn(sb.toString());
	}

	public int getInsertSyncLogThreadPoolSize() {
		return this.insertSyncLogThreadPoolSize;
	}

	public void setReplicationThreadPoolSize(int threadPoolSize) {
		StringBuilder sb = new StringBuilder("ReplicationThreadPoolSize [").append(this.threadPoolSize);
		sb.append("] switching to [").append(threadPoolSize).append("] ");

		this.threadPoolSize = threadPoolSize;
		this.replicationExecutor.setCorePoolSize(this.threadPoolSize);
		this.replicationExecutor.setMaximumPoolSize(this.threadPoolSize);

		sb.append("succeed.");
		logger.warn(sb.toString());
	}

	public int getReplicationThreadPoolSize() {
		return this.threadPoolSize;
	}

	public void setThreadPoolSize(int threadPoolSize) {
		this.threadPoolSize = threadPoolSize;
	}

	public void setWorkQueueSize(int workQueueSize) {
		this.workQueueSize = workQueueSize;
	}

	public void setInsertSyncLogWorkQueueSize(int insertSyncLogWorkQueueSize) {
		this.insertSyncLogWorkQueueSize = insertSyncLogWorkQueueSize;
	}

	public void setTaskListener(ReplicationTaskListener taskListener) {
		this.taskListener = taskListener;
	}

	public ReplicationTaskListener getTaskListener() {
		return taskListener;
	}

}
