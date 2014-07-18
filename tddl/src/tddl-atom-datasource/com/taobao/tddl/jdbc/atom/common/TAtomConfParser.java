package com.taobao.tddl.jdbc.atom.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.common.config.atom.TAtomConfHelper;
import com.taobao.tddl.jdbc.atom.config.object.TAtomDsConfDO;
import com.taobao.tddl.jdbc.atom.jdbc.ConnRestrictEntry;

/**
 * TAtom数据源的推送配置解析类
 * 
 * @author qihao
 *
 */
public class TAtomConfParser extends TAtomConfHelper{
	
	private static Log logger = LogFactory.getLog(TAtomConfParser.class);
	
	public static TAtomDsConfDO parserTAtomDsConfDO(String globaConfStr, String appConfStr) {
		TAtomDsConfDO pasObj = new TAtomDsConfDO();
		if (StringUtil.isNotBlank(globaConfStr)) {
			Properties globaProp = parserConfStr2Properties(globaConfStr);
			if (!globaProp.isEmpty()) {
				String ip = StringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_IP_KEY));
				if (StringUtil.isNotBlank(ip)) {
					pasObj.setIp(ip);
				}
				String port = StringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_PORT_KEY));
				if (StringUtil.isNotBlank(port)) {
					pasObj.setPort(port);
				}
				String dbName = StringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_DB_NAME_KEY));
				if (StringUtil.isNotBlank(dbName)) {
					pasObj.setDbName(dbName);
				}
				String dbType = StringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_DB_TYPE_KEY));
				if (StringUtil.isNotBlank(dbType)) {
					pasObj.setDbType(dbType);
				}
				String dbStatus = StringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_DB_STATUS_KEY));
				if (StringUtil.isNotBlank(dbStatus)) {
					pasObj.setDbStatus(dbStatus);
				}
			}
		}
		if (StringUtil.isNotBlank(appConfStr)) {
			Properties appProp = TAtomConfParser.parserConfStr2Properties(appConfStr);
			if (!appProp.isEmpty()) {
				String userName = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_USER_NAME_KEY));
				if (StringUtil.isNotBlank(userName)) {
					pasObj.setUserName(userName);
				}
				String oracleConType = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_ORACLE_CON_TYPE_KEY));
				if (StringUtil.isNotBlank(oracleConType)) {
					pasObj.setOracleConType(oracleConType);
				}
				String minPoolSize = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_MIN_POOL_SIZE_KEY));
				if (StringUtil.isNotBlank(minPoolSize)&&StringUtil.isNumeric(minPoolSize)) {
					pasObj.setMinPoolSize(Integer.valueOf(minPoolSize));
				}
				String maxPoolSize = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_MAX_POOL_SIZE_KEY));
				if (StringUtil.isNotBlank(maxPoolSize)&&StringUtil.isNumeric(maxPoolSize)) {
					pasObj.setMaxPoolSize(Integer.valueOf(maxPoolSize));
				}
				String idleTimeout = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_IDLE_TIMEOUT_KEY));
				if (StringUtil.isNotBlank(idleTimeout)&&StringUtil.isNumeric(idleTimeout)) {
					pasObj.setIdleTimeout(Long.valueOf(idleTimeout));
				}
				String blockingTimeout = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_BLOCKING_TIMEOUT_KEY));
				if (StringUtil.isNotBlank(blockingTimeout)&&StringUtil.isNumeric(blockingTimeout)) {
					pasObj.setBlockingTimeout(Integer.valueOf(blockingTimeout));
				}
				String preparedStatementCacheSize = StringUtil.trim(appProp
						.getProperty(TAtomConfParser.APP_PREPARED_STATEMENT_CACHE_SIZE_KEY));
				if (StringUtil.isNotBlank(preparedStatementCacheSize)&&StringUtil.isNumeric(preparedStatementCacheSize)) {
					pasObj.setPreparedStatementCacheSize(Integer.valueOf(preparedStatementCacheSize));
				}
				
				String writeRestrictTimes = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_WRITE_RESTRICT_TIMES));
				if(StringUtil.isNotBlank(writeRestrictTimes)&&StringUtil.isNumeric(writeRestrictTimes)){
					pasObj.setWriteRestrictTimes(Integer.valueOf(writeRestrictTimes));
				}
				
				String readRestrictTimes = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_READ_RESTRICT_TIMES));
				if(StringUtil.isNotBlank(readRestrictTimes)&&StringUtil.isNumeric(readRestrictTimes)){
					pasObj.setReadRestrictTimes(Integer.valueOf(readRestrictTimes));
				}
				String threadCountRestrict = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_THREAD_COUNT_RESTRICT));
				if(StringUtil.isNotBlank(threadCountRestrict)&&StringUtil.isNumeric(threadCountRestrict)){
					pasObj.setThreadCountRestrict(Integer.valueOf(threadCountRestrict));
				}
				String timeSliceInMillis = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_TIME_SLICE_IN_MILLS));
				if(StringUtil.isNotBlank(timeSliceInMillis)&&StringUtil.isNumeric(timeSliceInMillis)){
					pasObj.setTimeSliceInMillis(Integer.valueOf(timeSliceInMillis));
				}
				
				String conPropStr = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_CON_PROP_KEY));
				Map<String, String> connectionProperties = parserConPropStr2Map(conPropStr);
				if (null != connectionProperties
						&& !connectionProperties.isEmpty()) {
					pasObj.setConnectionProperties(connectionProperties);
					String driverClass = connectionProperties
							.get(TAtomConfParser.APP_DRIVER_CLASS_KEY);
					if (!StringUtil.isBlank(driverClass)) {
						pasObj.setDriverClass(driverClass);
					}
					
					// add by agapple, 简单处理支持下初始化链接
                    if(connectionProperties.containsKey(APP_PREFILL)){
                        String prefill = connectionProperties.remove(APP_PREFILL);
                        pasObj.setPrefill(BooleanUtils.toBoolean(prefill));
                        connectionProperties.remove(APP_PREFILL);
                    }
                    
                    String connectionInitSql = connectionProperties.remove(APP_CONNECTION_INIT_SQL_KEY);
                    if (!StringUtil.isBlank(connectionInitSql)) {
                        pasObj.setConnectionInitSql(connectionInitSql);
                    }
				}
				
				// 解析应用连接限制, 参看下面的文档
				String connRestrictStr = StringUtil.trim(appProp.getProperty(TAtomConfParser.APP_CONN_RESTRICT));
				List<ConnRestrictEntry> connRestrictEntries = parseConnRestrictEntries(connRestrictStr, 
						pasObj.getMaxPoolSize());
				if (null != connRestrictEntries && !connRestrictEntries.isEmpty()) {
					pasObj.setConnRestrictEntries(connRestrictEntries);
				}
			}
		}
		return pasObj;
	}

	/**
	 * HASH 策略的最大槽数量限制。
	 */
	public static final int MAX_HASH_RESTRICT_SLOT = 32;

	/**
	 * 解析应用连接限制, 完整格式是: "K1,K2,K3,K4:80%; K5,K6,K7,K8:80%; K9,K10,K11,K12:80%; *:16,80%; ~:80%;"
	 * 这样可以兼容  HASH: "*:16,80%", 也可以兼容  LIST: "K1:80%; K2:80%; K3:80%; K4:80%; ~:80%;"
	 * 配置可以是连接数, 也可以是百分比。
	 */
	public static List<ConnRestrictEntry> parseConnRestrictEntries(String connRestrictStr, int maxPoolSize) {
		List<ConnRestrictEntry> connRestrictEntries = null;
		if (StringUtil.isNotBlank(connRestrictStr)) {
			// Split "K1:number1; K2:number2; ...; *:count,number3; ~:number4"
			String[] entries = StringUtil.split(connRestrictStr, ";");
			if (null != entries && entries.length > 0) {
				HashMap<String, String> existKeys = new HashMap<String, String>();
				connRestrictEntries = new ArrayList<ConnRestrictEntry>(entries.length);
				for (String entry : entries) {
					// Parse "K1,K2,K3:number | *:count,number | ~:number"
					int find = entry.indexOf(':');
					if (find >= 1 && find < (entry.length() - 1)) {
						String key = entry.substring(0, find).trim();
						String value = entry.substring(find + 1).trim();
						// "K1,K2,K3:number | *:count,number | ~:number"
						ConnRestrictEntry connRestrictEntry = ConnRestrictEntry.parseEntry(
								key, value, maxPoolSize);
						if (connRestrictEntry == null) {
							logger.error("[connRestrict Error] parse entry error: " + entry);
						} else {
							// Remark entry config problem
							if (0 >= connRestrictEntry.getLimits()) {
								logger.error("[connRestrict Error] connection limit is 0: " + entry);
								connRestrictEntry.setLimits(/* 至少允许一个连接 */ 1);
							}
							if (ConnRestrictEntry.MAX_HASH_RESTRICT_SLOT < connRestrictEntry.getHashSize()) {
								logger.error("[connRestrict Error] hash size exceed maximum: " + entry);
								connRestrictEntry.setHashSize(ConnRestrictEntry.MAX_HASH_RESTRICT_SLOT);
							}
							// Remark Key config confliction
							for (String slotKey : connRestrictEntry.getKeys()) {
								if (!existKeys.containsKey(slotKey)) {
									existKeys.put(slotKey, entry);
								} else if (ConnRestrictEntry.isWildcard(slotKey)) {
									logger.error("[connRestrict Error] hash config ["
											+ entry + "] conflict with [" + existKeys.get(slotKey) + "]");
								} else if (ConnRestrictEntry.isNullKey(slotKey)) {
									logger.error("[connRestrict Error] null-key config ["
											+ entry + "] conflict with [" + existKeys.get(slotKey) + "]");
								} else {
									logger.error("[connRestrict Error] "
											+ slotKey + " config [" + entry + "] conflict with ["
											+ existKeys.get(slotKey) + "]");
								}
							}
							connRestrictEntries.add(connRestrictEntry);
						}
					} else {
						logger.error("[connRestrict Error] unknown entry: " + entry);
					}
				}
			}
		}
		return connRestrictEntries;
	}
	
}