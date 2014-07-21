//package com.taobao.tddl.client.jdbc.replication;
//
//import java.sql.SQLException;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.RejectedExecutionHandler;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import com.taobao.tddl.client.jdbc.SqlExecuteEvent;
//import com.taobao.tddl.client.util.LogUtils;
//import com.taobao.tddl.common.sync.RowBasedReplicationContext;
//import com.taobao.tddl.synccenter.SyncClient;
//import com.taobao.tddl.synccenter.message.Message;
//import com.taobao.tddl.synccenter.message.pb.protos.ReplicationProtos.UpdateReq;
//
///**
// * 
// * 异步请求行复制中心完成复制的行复制监听器
// * 
// * @author linxuan
// * 
// */
//public class AsyncCenterReplicationListener extends SyncCenterReplicationListener {
//	private static final Log logger = LogFactory.getLog(AsyncRowBasedReplicationListener.class);
//	private int insertSyncLogThreadPoolSize = 2;
//	private int insertSyncLogWorkQueueSize = 1024;
//	private ThreadPoolExecutor insertSyncLogExecutor;
//
//	private int sendThreadPoolSize = 1;
//	private int sendQueueSize = 1024;
//	private ExecutorService sendSyncRequestExecutor;
//
//	private static final Log localFailSyncLog = LogFactory.getLog(LogUtils.TDDL_LOCAL_FAIL_SYNC_LOG);
//
//	public void init() {
//		super.init();
//
//		insertSyncLogExecutor = new ThreadPoolExecutor(insertSyncLogThreadPoolSize, insertSyncLogThreadPoolSize, 0L,
//				TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(insertSyncLogWorkQueueSize),//
//				new RejectedExecutionHandler() {
//					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//						AsyncCenterReplicationListener.super.insertSyncLog2LocalFile(localFailSyncLog,
//								((InsertSyncLogTask) r).event);
//					}
//				});
//
//		sendSyncRequestExecutor = new ThreadPoolExecutor(sendThreadPoolSize, sendThreadPoolSize, 0L, TimeUnit.MILLISECONDS,
//				new ArrayBlockingQueue<Runnable>(sendQueueSize),//
//				new RejectedExecutionHandler() {
//					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//						logger.warn("A RowBasedReplicationTask discarded");
//					}
//				});
//	}
//
//	static class InsertSyncLogTask implements Runnable {
//		private final SqlExecuteEvent event;
//		private final RowBasedReplicationListener listener;
//
//		public InsertSyncLogTask(SqlExecuteEvent event, RowBasedReplicationListener listener) {
//			this.event = event;
//			this.listener = listener;
//		}
//
//		public void run() {
//			try {
//				// 尝试插入日志库
//				listener.insertSyncLog2Db(event);
//			} catch (SQLException e) {
//				// 失败时插入单独的log
//				RowBasedReplicationListener.insertSyncLog2LocalFile(localFailSyncLog, event);
//			}
//		}
//	}
//
//	private class SendSyncRequestTask implements Runnable {
//		private RowBasedReplicationContext context;
//
//		public SendSyncRequestTask(RowBasedReplicationContext context) {
//			this.context = context;
//		}
//
//		public void run() {
//			Message<UpdateReq> reqMsg = toUpdateReqMessage(context);
//			AsyncCenterReplicationListener.this.clientRandom.setSeed(System.currentTimeMillis());
//			SyncClient client = syncClients[clientRandom.nextInt(syncClients.length)];
//			client.sendSyncRequest(reqMsg);
//		}
//	}
//
//	@Override
//	protected void doAfterSqlExecute(RowBasedReplicationContext context) {
//		sendSyncRequestExecutor.execute(new SendSyncRequestTask(context));
//	}
//
//	@Override
//	protected void asyncInsertSyncLog2Db(SqlExecuteEvent event) {
//		// 先同步插入一条全量log
//		RowBasedReplicationListener.insertSyncLog2LocalFile(event);
//		insertSyncLogExecutor.execute(new InsertSyncLogTask(event, this));
//	}
//
//}
