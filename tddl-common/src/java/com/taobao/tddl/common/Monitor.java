package com.taobao.tddl.common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

import com.alibaba.common.lang.StringUtil;
import com.taobao.monitor.MonitorLog;
import com.taobao.tddl.common.ConfigServerHelper.AbstractDataListener;
import com.taobao.tddl.common.ConfigServerHelper.DataListener;
import com.taobao.tddl.common.ConfigServerHelper.TDDLConfigKey;
import com.taobao.tddl.common.monitor.SnapshotValuesOutputCallBack;
import com.taobao.tddl.common.monitor.stat.AbstractStatLogWriter.LogCounter;
import com.taobao.tddl.common.monitor.stat.BufferedLogWriter;
import com.taobao.tddl.common.monitor.stat.Log4jLogWriter;
import com.taobao.tddl.common.monitor.stat.MinMaxAvgLogWriter;
import com.taobao.tddl.common.monitor.stat.SoftRefLogWriter;
import com.taobao.tddl.common.util.BoundedConcurrentHashMap;
import com.taobao.tddl.common.util.NagiosUtils;
import com.taobao.tddl.common.util.TStringUtil;

public class Monitor {
	public static final String KEY1 = "TDDL";
	//public static final String KEY1_TABLE = "TDDL_TABLE|";
	//public static final String KEY2_EXEC_SQL = "TDDL_SQL|";
	public static final String KEY2_SYNC = "Sync";
	public static final String KEY2_SYNC_CONTEXT_SQL = "SyncServerContextSql"; //added by huali，是在同步服务器同步的时候设置给context的sql，否则监控的代码会NPE 
	public static final String KEY3_BatchUpdateSyncLog = "BatchUpdateSyncLog";
	public static final String KEY3_BatchDeleteSyncLog = "BatchDeleteSyncLog";
	public static final String KEY3_SyncLogFetched = "SyncLogFetched";
	public static final String KEY3_ReplicationTasksAccepted = "ReplicationTasksAccepted";
	public static final String KEY3_UpdateSlaveRow_dup_all = "UpdateSlaveRow_dup_all";
	public static final String KEY3_PARSE_SQL = "PARSE_SQL_SUCCESS";
	public static final String KEY3_TAIR_HIT_RATING = "TAIR_HIT_RATING";
	public static final String KEY3_GET_DB_AND_TABLES = "GET_DB_ANDTABLES_SUCCESS";//会记录走则引擎用的时间和总耗时
	/**
	 * 执行sql的总时间，包含真正数据库的执行时间和总时间
	 */
	public static final String KEY3_EXECUTE_A_SQL_SUCCESS = "EXECUTE_A_SQL_SUCCESS";
	/**
	 * 总共执行了几个库，几个表
	 */
	public static final String KEY3_EXECUTE_A_SQL_SUCCESS_DBTAB = "EXECUTE_A_SQL_SUCCESS_DBTAB";
	/**
	 * 执行sql的总时间，包含真正数据库的执行时间和总时间
	 */
	public static final String KEY3_EXECUTE_A_SQL_TIMEOUT = "EXECUTE_A_SQL_TIMEOUT";

	public static final String KEY3_EXECUTE_A_SQL_TIMEOUT_DBTAB = "EXECUTE_A_SQL_TIMEOUT_DBTAB";

	public static final String KEY3_EXECUTE_A_SQL_EXCEPTION = "EXECUTE_A_SQL_WITH_EXCEPTION";

	public static final String KEY3_EXECUTE_A_SQL_EXCEPTION_DBTAB = "EXECUTE_A_SQL_WITH_EXCEPTION_DBTAB";

	public static final String KEY2_REPLICATION_SQL = "TDDL_REPLICATION_SQL|";

	/**
	 * 复制到从库成功，计入写库时间和总耗费时间
	 */
	public static final String KEY3_COPY_2_SLAVE_SUCCESS = "COPY_2_SLAVE_SUCCESS";

	/**
	 * 记录从生成任务到该任务开始被执行之间所消耗的时间
	 */
	public static final String KEY3_COPY_2_SLAVE_SUCCESS_TIME_CONSUMING_IN_THREADPOOL = "COPY_2_SLAVE_SUCCESS_TIME_CONSUMING_IN_THREADPOOL";
	/**
	 * 复制到从库超时，要记录查询+写入sql所耗费的时间。
	 */
	public static final String KEY3_COPY_2_SLAVE_TIMEOUT = "COPY_2_SLAVE_TIMEOUT";

	/**
	 * 记录从生成任务到该任务开始被执行之间所消耗的时间
	 */
	public static final String KEY3_COPY_2_SLAVE_TIMEOUT_TIME_CONSUMING_IN_THREADPOOL = "COPY_2_SLAVE_TIMEOUT_TIME_CONSUMING_IN_THREADPOOL";
	/**
	 * 复制到从库异常，不计入主键冲突认为更新成功这种情况。
	 */
	public static final String KEY3_COPY_2_SLAVE_EXCEPTION = "COPY_2_SLAVE_EXCEPTION";

	public static final String KEY3_COPY_2_SLAVE_EXCEPTION_TIME_CONSUMING_IN_THREADPOOL = "COPY_2_SLAVE_EXCEPTION_TIME_CONSUMING_IN_THREADPOOL";

	/**
	 * 使用syncCenter，复制成功总耗费时间（请求syncCenter前，到syncCenter返回响应后）
	 */
	public static final String KEY3_SYNC_VIA_CENTER_SUCCESS = "SYNC_VIA_CENTER_SUCCESS";
	/**
	 * 使用syncCenter，复制超时时间（请求syncCenter前，到syncCenter返回响应后）
	 */
	public static final String KEY3_SYNC_VIA_CENTER_TIMEOUT = "SYNC_VIA_CENTER_TIMEOUT";
	/**
	 * 使用syncCenter，复制超时时，任务在队列中的等待时间
	 */
	public static final String KEY3_SYNC_VIA_CENTER_TIMEOUT_TIME_IN_QUEUE = "SYNC_VIA_CENTER_TIMEOUT_TIME_IN_QUEUE";
	/**
	 * value1：使用syncCenter，在超时间waitForResponseTimeout内,没有等到server返回的次数。value2：总次数
	 */
	public static final String KEY3_SYNC_VIA_CENTER_NO_RESPONSE = "SYNC_VIA_CENTER_NO_RESPONSE";
	
	/**
	 * TDDL 数据库（分桶）连接数
	 */
	public static final String KEY3_CONN_NUMBER = "CONN_NUM";
	/**
	 * TDDL 数据库（分桶）连接阻塞时间
	 */
	public static final String KEY3_CONN_BLOCKING = "CONN_BLOCKING";
	
	/** changyuan.lh: TDDL 统计日志 */
	private static final BufferedLogWriter bufferedStatLogWriter = new BufferedLogWriter(
			/* XXX: 记录行复制日志与 SQL 解析日志, Key 的量与 SQL 数量相同 */
			1024, 4096, new Log4jLogWriter(LoggerInit.TDDL_Statistic_LOG));
	private static final BufferedLogWriter atomBufferedStatLogWriter = new BufferedLogWriter(
			/* XXX: 记录单库的 SQL 执行记录, Key 的量是 SQL x 单库物理表 x 物理库数量 */
			2048, 131072, new Log4jLogWriter(LoggerInit.TDDL_Atom_Statistic_LOG));
	private static final BufferedLogWriter matrixBufferedStatLogWriter = new BufferedLogWriter(
			/* XXX: 记录逻辑表以及物理库/物理表 的 SQL 执行记录, Key 量最大是 SQL x 单库物理表 x 物理库数量 */
			2048, 131072, new Log4jLogWriter(LoggerInit.TDDL_Matrix_Statistic_LOG));
	private static final SoftRefLogWriter connRefStatLogWriter = new SoftRefLogWriter(
			/* XXX: 记录 Atom 连接池以及业务分桶的连接申请记录, Key 量最大是物理库 x 业务分桶数量 */
			false, new MinMaxAvgLogWriter(", ", LoggerInit.TDDL_Conn_Statistic_LOG));
	
	/**
	 * 记log的时间
	 */
	public static final String KEY3_WRITE_LOG_SUCCESS = "WRITE_LOG_SUCCESS";

	public static final String KEY3_WRITE_LOG_EXCEPTION = "WRITE_LOG_EXCEPTION";

	private static final Log logger = LogFactory.getLog(Monitor.class);
	private static final Logger log = LoggerInit.TDDL_MD5_TO_SQL_MAPPING;
	private static final BoundedConcurrentHashMap<String, String> sqlToMD5Map = new BoundedConcurrentHashMap<String, String>();
	private static MD5Maker md5Maker = MD5Maker.getInstance();
	public static volatile String APPNAME = "TDDL";

	public enum RECORD_TYPE {
		RECORD_SQL, MD5, NONE
	}

	private static volatile RECORD_TYPE recordType = RECORD_TYPE.RECORD_SQL;
	private static volatile int left = 0; //从左起保留多少个字符
	private static volatile int right = 0;//从右起保留多少个字符
	private static volatile String[] excludsKeys = null;
	private static volatile String[] includeKeys = null; //白名单
	public static volatile Boolean isStatRealDbInWrapperDs = null;
	//modify by junyu,2012-3-28
	public static volatile boolean isStatAtomSql = true; //默认不打印sql日志
	public static volatile int sqlTimeout=500; //默认超时500毫秒
	public static volatile int atomSamplingRate=100;//值只能为0-100,日志的采样频率
	public static volatile int statChannelMask = 7; //按位：哈勃|BufferedStatLogWriter|StatMonitor
	public static volatile int dumpInterval = -1;
	public static volatile int cacheSize = -1;
	static {
		init();
	}

	//private static AsynWriter<String> inputWriter;
	private static void init() {
		// changyuan.lh: 初始化 TDDL 日志, BufferedStatLogWriter 移到这里
		LoggerInit.initTddlLog();

		if ("TDDL".equals(APPNAME)) {
			logger.warn("不指定TDDL以外的appName则不订阅");
			return;
		}
		//DATA_ID_TDDL_CLIENT_CONFIG = DATA_ID_PREFIX + "{0}_tddlconfig"
		//String tddlconfigDataId = ConfigServerHelper.DATA_ID_PREFIX + APPNAME + "_tddlconfig";
		//Object firstFetchedConfigs = ConfigServerHelper.subscribePersistentData(tddlconfigDataId, tddlConfigListener);
		Object firstFetchedConfigs = ConfigServerHelper.subscribeTDDLConfig(APPNAME, tddlConfigListener);
		if (firstFetchedConfigs == null) {
			logger.warn("No tddlconfig received, use default");
		}
		//inputWriter = new AsynWriter<String>(commalog);
		//inputWriter.init();
	}

	public static interface GlobalConfigListener {
		void onConfigReceive(Properties p);
	}

	private static final Set<GlobalConfigListener> globalConfigListeners = new HashSet<GlobalConfigListener>(0);

	public static void addGlobalConfigListener(GlobalConfigListener listener) {
		globalConfigListeners.add(listener);
	}
	public static void removeGlobalConfigListener(GlobalConfigListener listener) {
		globalConfigListeners.remove(listener);
	}

	private static final DataListener tddlConfigListener = new AbstractDataListener() {
		public void onDataReceive(Object data) {
			Properties p = ConfigServerHelper.parseProperties(data, "[tddlConfigListener]");
			if (p == null) {
				logger.warn("Empty tddlconfig");
				return;
			}
			try {
				for (Map.Entry<Object, Object> entry : p.entrySet()) {
					String key = ((String) entry.getKey()).trim();
					String value = ((String) entry.getValue()).trim();
					switch (TDDLConfigKey.valueOf(key)) {
					case statKeyRecordType: {
						RECORD_TYPE old = recordType;
						recordType = RECORD_TYPE.valueOf(value);
						logger.warn("statKeyRecordType switch from [" + old + "] to [" + recordType + "]");
						break;
					}
					case statKeyLeftCutLen: {
						int old = left;
						left = Integer.valueOf(value);
						logger.warn("statKeyLeftCutLen switch from [" + old + "] to [" + left + "]");
						break;
					}
					case statKeyRightCutLen: {
						int old = right;
						right = Integer.valueOf(value);
						logger.warn("statKeyRightCutLen switch from [" + old + "] to [" + right + "]");
						break;
					}
					case statKeyExcludes: {
						String[] old = excludsKeys;
						excludsKeys = value.split(",");
						logger.warn("statKeyExcludes switch from " + Arrays.toString(old) + " to [" + value + "]");
						break;
					}
					case statKeyIncludes: {
						String[] old = includeKeys;
						includeKeys = value.split(",");
						logger.warn("statKeyIncludes switch from " + Arrays.toString(old) + " to [" + value + "]");
						break;
					}
					case StatRealDbInWrapperDs: {
						boolean old = isStatRealDbInWrapperDs;
						isStatRealDbInWrapperDs = Boolean.valueOf(value);
						logger.warn("StatRealDbInWrapperDs switch from [" + old + "] to [" + value + "]");
						break;
					}
					case StatChannelMask: {
						int old = statChannelMask;
						statChannelMask = Integer.valueOf(value);
						logger.warn("statChannelMask switch from [" + old + "] to [" + value + "]");
						break;
					}
					case statDumpInterval: {
						int old = dumpInterval;
						dumpInterval = Integer.valueOf(value);
						statMonitor.setFlushInterval(dumpInterval);
						bufferedStatLogWriter.setFlushInterval(dumpInterval);
						logger.warn("statDumpInterval switch from [" + old + "] to [" + value + "]");
						break;
					}
					case statCacheSize: {
						int old = cacheSize;
						cacheSize = Integer.valueOf(value);
						statMonitor.setMaxKeySize(cacheSize);
						bufferedStatLogWriter.setMaxKeySize(cacheSize);
						logger.warn("statCacheSize switch from [" + old + "] to [" + value + "]");
						break;
					}
					case statAtomSql: {
						boolean old = isStatAtomSql;
						isStatAtomSql = Boolean.parseBoolean(value);
						logger.warn("isStatAtomSql switch from [" + old + "] to [" + value + "]");
						break;
					}
					case sqlExecTimeOutMilli:{
						int old = sqlTimeout;
						sqlTimeout=Integer.valueOf(value);
						logger.warn("sqlTimeout switch from [" + old + "] to [" + value + "]");
						break;
					}
					case atomSqlSamplingRate:{
						int old=atomSamplingRate;
						if(old>0){
							int rate=0;
							if(Integer.valueOf(value) % 100==0){
								rate=100;
							}else{
								rate=Integer.valueOf(value) % 100;//如果超过100,取余量
							}
							atomSamplingRate=rate;
							logger.warn("atomSqlSamplingRate switch from [" + old + "] to [" + atomSamplingRate + "]");
						}else{
							logger.warn("atomSqlSamplingRate will not change,because the value got is nagetive!old value is:"+old);
						}
					}
					default:
						logger.warn("Not cared TDDLConfigKey:" + key);
					}
				}
			} catch (Exception e) {
				logger.error("[tddlConfigListener.onDataReceive]", e);
			}
			
			for (GlobalConfigListener listener : globalConfigListeners) {
				listener.onConfigReceive(p);
			}
		}
	};
	//public static final StatMonitor statMonitor = StatMonitor.getInstance();
	public static final StatMonitor statMonitor = StatMonitor.getInstance();

	private static void addMonitor(String key1, String key2, String key3, long value1, long value2) {
		//一段时间内插日志库的失败率和平均响应时间
		if (KEY3_WRITE_LOG_SUCCESS.equals(key3)) {
			statMonitor.addStat(key1, "", NagiosUtils.KEY_INSERT_LOGDB_FAIL_RATE, 0);
			statMonitor.addStat(key1, "", NagiosUtils.KEY_INSERT_LOGDB_TIME_AVG, value1);
		} else if (KEY3_WRITE_LOG_EXCEPTION.equals(key3)) {
			statMonitor.addStat(key1, "", NagiosUtils.KEY_INSERT_LOGDB_FAIL_RATE, 1);
		}
		//一段时间内行复制的失败率和平均响应时间
		else if (KEY3_COPY_2_SLAVE_SUCCESS.equals(key3)) {
			statMonitor.addStat(key1, "", NagiosUtils.KEY_REPLICATION_FAIL_RATE, 0);
			statMonitor.addStat(key1, "", NagiosUtils.KEY_REPLICATION_TIME_AVG, value1);
		} else if (KEY3_WRITE_LOG_EXCEPTION.equals(key3)) {
			statMonitor.addStat(key1, "", NagiosUtils.KEY_REPLICATION_FAIL_RATE, 1);
		}
	}

	public static String buildTableKey1(String virtualTableName) {
		//return KEY1_TABLE+virtualTableName;
		return "" + virtualTableName; //保证不返回null
	}

	/**
	 * 记录sql
	 * 不记录sql
	 * 记录前截取sql
	 * 记录后截取sql
	 * 记录md5
	 * 
	 * 先左后右
	 * 
	 * @param sql
	 * @return
	 */
	public static String buildExecuteSqlKey2(String sql) {
		if (sql == null) {
			return "null";
		}
		switch (recordType) {
		case RECORD_SQL:
			String s = TStringUtil.fillTabWithSpace(sql);
			if (left > 0) {
				s = StringUtil.left(s, left);
			}
			if (right > 0) {
				s = StringUtil.right(s, right);
			}
			return s;
		case MD5:
			String s1 = TStringUtil.fillTabWithSpace(sql);
			if (left > 0) {
				s1 = StringUtil.left(s1, left);
			}
			if (right > 0) {
				s1 = StringUtil.right(s1, right);
			}
			String md5 = sqlToMD5Map.get(s1);
			if (md5 != null) {
				return md5;
			} else {
				String sqlmd5 = md5Maker.getMD5(s1);
				StringBuilder sb = new StringBuilder();
				sb.append("[md5]").append(sqlmd5).append(" [sql]").append(s1);
				log.warn(sb.toString());
				sqlToMD5Map.put(s1, sqlmd5);
				return sqlmd5;
			}
		case NONE:
			return "";
		default:
			throw new IllegalArgumentException("不符合要求的记录log类型! " + recordType);
		}

	}

	public static String buildExecuteDBAndTableKey1(String realDSKey, String realTable) {
		StringBuilder sb = new StringBuilder();
		sb.append(KEY1).append("|").append(realDSKey).append("|").append(realTable);
		return sb.toString();
	}

	/**
	 * 数据复制过程中需要用到的sql的key
	 * 
	 * @param sql
	 * @return
	 */
	public static String buildReplicationSqlKey2(String sql) {
		return buildExecuteSqlKey2(sql);
	}

	/**
	 * @param key1 一般是逻辑表名，appname等
	 * @param key2 一般是SQL
	 * @param key3 一些成功、失败、超时、命中率等标志
	 * @param value1 执行时间
	 * @param value2 次数
	 */
	public static void add(String key1, String key2, String key3, long value1, long value2) {
		if (isExclude(key1, key2, key3)) {
			return;
		}
		if ((statChannelMask & 4) == 4) { // 100
			MonitorLog.addStat(key1, "", key3, value1, value2); // 哈勃日志暂时保留
		}
		if ((statChannelMask & 2) == 2) { // 010
			bufferedStatLogWriter.stat(key2, key1, key3, value2, value1); //
		}
		if ((statChannelMask & 1) == 1) { // 001
			addMonitor(key1, key2, key3, value1, value2); // 平均响应时间等动态监控Nagois
		}
	}
	
	public static void atomSqlAdd(String key1,String key2,String key3,String key4,String key5,String key6,long value1,long value2){
		// changyuan.lh XXX: 输出顺序是  key1(sql), key2(group), attach1, attach2, attach3, key3(flag)
		atomBufferedStatLogWriter.write(new Object[] { key2, key1, key3 }, 
				new Object[] { key2, key1, key4, key5, key6, key3 }, new long[] { value2, value1 });
	}
	
	public static void matrixSqlAdd(String key1,String key2,String key3,long value1,long value2){
		matrixBufferedStatLogWriter.stat(key2, key1, key3, value2, value1);
	}
	
	// 获得一个统计对象, 不用可以直接抛弃
	public static LogCounter connStat(String obj1, String obj2, String obj3) {
		Object[] objs = new Object[] { obj1, obj2, obj3 };
		return connRefStatLogWriter.getCounter(objs, objs);
	}
	
	private final static PositiveAtomicCounter pc=new PositiveAtomicCounter();
	public static boolean isSamplingRecord(){
		int ra=pc.incrementAndGet()%100;
		if(ra<Monitor.atomSamplingRate){
			return true;
		}else{
			return false;
		}
	}

	private static boolean isExclude(String key1, String key2, String key3) {
		if (excludsKeys == null || excludsKeys.length == 0)
			return false;
		for (String exclude : excludsKeys) {
			if (key1.indexOf(exclude) != -1 || key2.indexOf(exclude) != -1 || key3.indexOf(exclude) != -1)
				return true;
		}
		return false;
	}

	public static boolean isInclude(String sql) {
		if (includeKeys != null && includeKeys.length != 0) { // 存在白名单
			boolean discard = true;
			for (String whiteItem : includeKeys) {
				if (sql.indexOf(whiteItem) != -1) {
					discard = false;
					break;
				}
			}
			if (discard) {
				return false; // 不在白名单中，不输出日志，以减少日志量
			}
		}
		return true;
	}

	public static void setAppName(String appname) {
		if (appname != null) {
			APPNAME = appname;
			init();
		}
	}

	public static synchronized void addSnapshotValuesCallbask(SnapshotValuesOutputCallBack callbackList) {
		statMonitor.addSnapshotValuesCallbask(callbackList);
	}

	public static synchronized void removeSnapshotValuesCallback(SnapshotValuesOutputCallBack callbackList) {
		statMonitor.removeSnapshotValuesCallback(callbackList);
	}
}
