//package com.taobao.tddl.client.jdbc.replication;
//
//import static com.taobao.tddl.common.Monitor.add;
//import static com.taobao.tddl.common.Monitor.buildReplicationSqlKey2;
//import static com.taobao.tddl.common.Monitor.buildTableKey1;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Random;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import com.taobao.tddl.client.jdbc.SqlExecuteEvent;
//import com.taobao.tddl.client.jdbc.TDataSource;
//import com.taobao.tddl.common.Monitor;
//import com.taobao.tddl.common.sync.RowBasedReplicationContext;
//import com.taobao.tddl.synccenter.SyncClient;
//import com.taobao.tddl.synccenter.message.DefaultMessageRecognizer;
//import com.taobao.tddl.synccenter.message.Message;
//import com.taobao.tddl.synccenter.message.MessageParser;
//import com.taobao.tddl.synccenter.message.MessageRecognizer;
//import com.taobao.tddl.synccenter.message.pb.UpdateReqMessage;
//import com.taobao.tddl.synccenter.message.pb.UpdateRspMessage;
//import com.taobao.tddl.synccenter.message.pb.protos.ReplicationProtos.SqlType;
//import com.taobao.tddl.synccenter.message.pb.protos.ReplicationProtos.UpdateReq;
//import com.taobao.tddl.synccenter.message.pb.protos.ReplicationProtos.UpdateRsp;
//
///**
// * 
// * 同步的请求行复制中心完成复制的行复制监听器
// * 
// * @author linxuan
// *
// */
//public class SyncCenterReplicationListener extends RowBasedReplicationListener {
//	
//	private static final Log log = LogFactory.getLog(SyncCenterReplicationListener.class);
//
//	private static final long timeoutThreshold = 200; //复制超时统计的时间阀值
//	private MessageRecognizer messageRecognizer;
//	private Map<Integer, MessageParser<?>> messageParsers;
//
//	private String syncCenterAddressList; //若不注入，则会使用订阅的
//	private int connectionCount = 1;
//	private int waitForResponseTimeoutMillisecond = 1000; //这个值的设置要大于timeoutThreshold
//	private int sleepIntervalOnIdle = 2; //空闲时的睡眠时间
//	private boolean waitResponse = true;
//
//	protected SyncClient[] syncClients;
//	protected Random clientRandom = new Random();
//	private static AtomicLong reqSeqNo = new AtomicLong(1);
//
//	@Override
//	public void init(TDataSource tds) {
//		//initSyncLogDb(tds.isUseLocalConfig());
//		
//		if (messageRecognizer == null) {
//			messageRecognizer = new DefaultMessageRecognizer();
//			if (messageParsers == null) {
//				messageParsers = new HashMap<Integer, MessageParser<?>>(1,1);
//				messageParsers.put(UpdateRspMessage.parser.getMsgCode(), UpdateRspMessage.parser);
//			}
//			((DefaultMessageRecognizer) messageRecognizer).setMessageParsers(messageParsers);
//		}
//
//		syncClients = new SyncClient[connectionCount];
//		for (int i = 0; i < connectionCount; i++) {
//			syncClients[i] = buildSyncClient();
//		}
//	}
//
//	private SyncClient buildSyncClient() {
//		SyncClient sc = new SyncClient();
//		sc.setWaitResponse(waitResponse);
//		sc.setMessageRecognizer(messageRecognizer);
//		sc.setSyncCenterAddressList(syncCenterAddressList);
//		//sc.setSleepIntervalOnIdle(this.sleepIntervalOnIdle);
//		Properties clientConfig = new Properties();
//		clientConfig.put("SleepIntervalOnIdle", this.sleepIntervalOnIdle );
//		sc.setClientConfig(clientConfig);
//		sc.init();
//		return sc;
//	}
//
//	@Override
//	protected void doAfterSqlExecute(RowBasedReplicationContext context) {
//		Message<UpdateReq> reqMsg = toUpdateReqMessage(context);
//		clientRandom.setSeed(System.currentTimeMillis());
//		SyncClient client = syncClients[clientRandom.nextInt(syncClients.length)];
//
//		Message<UpdateRsp> rspMsg = client.waitForResponse(reqMsg, waitForResponseTimeoutMillisecond);
//		
//		String key1 = buildTableKey1(context.getMasterLogicTableName());
//		String key2 = buildReplicationSqlKey2(context.getSql());
//		add(key1, key2, Monitor.KEY3_SYNC_VIA_CENTER_NO_RESPONSE, 0, 1);
//
//		if(rspMsg == null){
//			//等待synccenter返回响应超时
//			add(key1, key2, Monitor.KEY3_SYNC_VIA_CENTER_NO_RESPONSE, 1, 0);
//			return;
//		}
//		UpdateRsp rsp = rspMsg.getMessageInstance();
//		if (rsp.getResultCode() == 0) {
//			//复制成功
//			long elapsedTime = System.currentTimeMillis() - context.getAfterMainDBSqlExecuteTime();
//			long timeConsumingInThreadPool = context.getReplicationStartTime() - context.getAfterMainDBSqlExecuteTime();
//			successProfile(key1,key2, timeoutThreshold, elapsedTime, timeConsumingInThreadPool);
//		} else {
//			//复制失败
//			log.warn("Sync via center failed. resultCode:" + rsp.getResultCode() + ",resultMessage:"
//					+ rsp.getResultMessage());
//		}
//	}
//	
//	protected Message<UpdateReq> toUpdateReqMessage(RowBasedReplicationContext r) {
//		UpdateReq.Builder context = UpdateReq.newBuilder();
//
//		context.setReqSeqNo(reqSeqNo.getAndIncrement());
//		context.setSqlType(SqlType.valueOf(r.getSqlType().toString()));
//		context.setSyncLogId(r.getSyncLogId());
//		context.setSyncLogDsKey(r.getSyncLogDsKey());
//		context.setMasterLogicTableName(r.getMasterLogicTableName());
//
//		context.setPrimaryKeyColumn(r.getPrimaryKeyColumn());
//		context.setPrimaryKeyValue(Long.valueOf(r.getPrimaryKeyValue().toString()));//若不是Long直接抛错
//
//		if (r.getMasterDatabaseShardColumn() != null) {
//			context.setMasterDatabaseShardColumn(r.getMasterDatabaseShardColumn());
//		}
//		if (r.getMasterDatabaseShardValue() != null) {
//			context.setMasterDatabaseShardValue(Long.valueOf(r.getMasterDatabaseShardValue().toString()));//若不是Long直接抛错
//		}
//
//		if (r.getMasterTableShardColumn() != null) {
//			context.setMasterTableShardColumn(r.getMasterTableShardColumn());
//		}
//		if (r.getMasterTableShardValue() != null) {
//			context.setMasterTableShardValue(Long.valueOf(r.getMasterTableShardValue().toString()));//若不是Long直接抛错
//		}
//
//		return new UpdateReqMessage(context.build());
//	}
//
//	@Override
//	public RowBasedReplicationContext buildRowBasedReplicationContext(SqlExecuteEvent event) {
//		RowBasedReplicationContext context = new RowBasedReplicationContext();
//		context.setSqlType(event.getSqlType());
//		context.setPrimaryKeyColumn(event.getPrimaryKeyColumn());
//		context.setPrimaryKeyValue(event.getPrimaryKeyValue());
//		context.setMasterLogicTableName(event.getLogicTableName());
//		context.setMasterDatabaseShardColumn(event.getDatabaseShardColumn());
//		context.setMasterDatabaseShardValue(event.getDatabaseShardValue());
//		context.setMasterTableShardColumn(event.getTableShardColumn());
//		context.setMasterTableShardValue(event.getTableShardValue());
//		context.setSyncLogJdbcTemplate(event.getSyncLogJdbcTemplate());
//		context.setSyncLogId(event.getSyncLogId());
//		context.setSyncLogDsKey(event.getSyncLogDsKey()); //for support sync center
//        
//		//这里没有replicationConfig，从而也没有BizTDDLContext及相关属性的设置
//		context.setAfterMainDBSqlExecuteTime(event.getAfterMainDBSqlExecuteTime());
//		context.setSql(event.getSql());
//		return context;
//	}
//
//	@Override
//	protected void asyncInsertSyncLog2Db(SqlExecuteEvent event) {
//		throw new UnsupportedOperationException("asyncInsertSyncLog2Db:同步Listener不支持异步插入日志库");
//	}
//
//	/**
//	 * 
//	 * @param context
//	 * @param timeoutThreshold
//	 * @param elapsedTime 从主库更新成功，到收到syncCenter复制完成响应的时间
//	 * @param timeConsumingInThreadPool
//	 */
//	private static void successProfile(String key1, String key2, long timeoutThreshold, long elapsedTime,
//			long timeConsumingInThreadPool) {
//		if (elapsedTime > timeoutThreshold) {
//			// timeout
//			add(key1, key2, Monitor.KEY3_SYNC_VIA_CENTER_TIMEOUT, elapsedTime, 1);
//			add(key1, key2, Monitor.KEY3_SYNC_VIA_CENTER_TIMEOUT_TIME_IN_QUEUE, timeConsumingInThreadPool, 1);
//		} else {
//			// normal
//			add(key1, key2, Monitor.KEY3_SYNC_VIA_CENTER_SUCCESS, elapsedTime, 1);
//		}
//	}
//
//	/**
//	 * 无逻辑的getters/setters
//	 */
//	public void setMessageRecognizer(MessageRecognizer messageRecognizer) {
//		this.messageRecognizer = messageRecognizer;
//	}
//
//	public void setMessageParsers(Map<Integer, MessageParser<?>> messageParsers) {
//		this.messageParsers = messageParsers;
//	}
//
//	public void setSyncCenterAddressList(String syncCenterAddressList) {
//		this.syncCenterAddressList = syncCenterAddressList;
//	}
//
//	public void setConnectionCount(int connectionCount) {
//		this.connectionCount = connectionCount;
//	}
//
//	public void setWaitForResponseTimeoutMillisecond(int waitForResponseTimeoutMillisecond) {
//		this.waitForResponseTimeoutMillisecond = waitForResponseTimeoutMillisecond;
//	}
//
//	public void setSleepIntervalOnIdle(int sleepIntervalOnIdle) {
//		this.sleepIntervalOnIdle = sleepIntervalOnIdle;
//	}
//
//	public void setWaitResponse(boolean waitResponse) {
//		this.waitResponse = waitResponse;
//	}
//}
