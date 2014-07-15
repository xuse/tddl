package com.taobao.tddl.common.config.atom;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.alibaba.common.lang.io.ByteArrayInputStream;
import com.taobao.tddl.common.securety.SecureIdentityLoginModule;

/**
 * TAtom数据源的推送配置解析类
 * 
 * @author qihao
 *
 */
public class TAtomConfHelper {
	private static Log logger = LogFactory.getLog(TAtomConfHelper.class);

	public static final String GLOBA_IP_KEY = "ip";
	public static final String GLOBA_PORT_KEY = "port";
	public static final String GLOBA_DB_NAME_KEY = "dbName";
	public static final String GLOBA_DB_TYPE_KEY = "dbType";
	public static final String GLOBA_DB_STATUS_KEY = "dbStatus";
	public static final String APP_USER_NAME_KEY = "userName";
	public static final String APP_MIN_POOL_SIZE_KEY = "minPoolSize";
	public static final String APP_MAX_POOL_SIZE_KEY = "maxPoolSize";
	public static final String APP_INIT_POOL_SIZE_KEY = "initPoolSize";
	public static final String APP_PREFILL = "prefill";
	public static final String APP_IDLE_TIMEOUT_KEY = "idleTimeout";
	public static final String APP_BLOCKING_TIMEOUT_KEY = "blockingTimeout";
	public static final String APP_PREPARED_STATEMENT_CACHE_SIZE_KEY = "preparedStatementCacheSize";
	public static final String APP_ORACLE_CON_TYPE_KEY = "oracleConType";
	public static final String APP_CON_PROP_KEY = "connectionProperties";
	public static final String PASSWD_ENC_PASSWD_KEY = "encPasswd";
	public static final String PASSWD_ENC_KEY_KEY = "encKey";
	
	public static final String APP_DRIVER_CLASS_KEY = "driverClass";
	/**
	 * 写，次数限制
	 */
	public static final String APP_WRITE_RESTRICT_TIMES = "writeRestrictTimes";
	/**
	 * 读，次数限制
	 */
	public static final String APP_READ_RESTRICT_TIMES = "readRestrictTimes";
	/**
	 * thread count 次数限制
	 */
	public static final String APP_THREAD_COUNT_RESTRICT = "threadCountRestrict";
	
	public static final String APP_TIME_SLICE_IN_MILLS = "timeSliceInMillis";

	/**
	 * 应用连接限制: 限制某个应用键值的并发连接数。
	 */
	public static final String APP_CONN_RESTRICT = "connRestrict";
	
	public static final String APP_CONNECTION_INIT_SQL_KEY = "connectionInitSql";

	public static Map<String, String> parserConPropStr2Map(String conPropStr) {
		Map<String, String> connectionProperties = null;
		if (StringUtil.isNotBlank(conPropStr)) {
			String[] keyValues = StringUtil.split(conPropStr, ";");
			if (null != keyValues && keyValues.length > 0) {
				connectionProperties = new HashMap<String, String>(keyValues.length);
				for (String keyValue : keyValues) {
					String key = StringUtil.substringBefore(keyValue, "=");
					String value = StringUtil.substringAfter(keyValue, "=");
					if (StringUtil.isNotBlank(key) && StringUtil.isNotBlank(value)) {
						connectionProperties.put(key.trim(), value.trim());
					}
				}
			}
		}
		return connectionProperties;
	}

	public static String parserPasswd(String passwdStr) {
		String passwd = null;
		Properties passwdProp = TAtomConfHelper.parserConfStr2Properties(passwdStr);
		String encPasswd = passwdProp.getProperty(TAtomConfHelper.PASSWD_ENC_PASSWD_KEY);
		if (StringUtil.isNotBlank(encPasswd)) {
			String encKey = passwdProp.getProperty(TAtomConfHelper.PASSWD_ENC_KEY_KEY);
			try {
				passwd = SecureIdentityLoginModule.decode(encKey, encPasswd);
			} catch (Exception e) {
				logger.error("[parserPasswd Error] decode dbPasswdError!may jdk version error!", e);
			}
		}
		return passwd;
	}

	public static Properties parserConfStr2Properties(String data) {
		Properties prop = new Properties();
		if (StringUtil.isNotBlank(data)) {
			ByteArrayInputStream byteArrayInputStream = null;
			try {
				byteArrayInputStream = new ByteArrayInputStream((data).getBytes());
				prop.load(byteArrayInputStream);
			} catch (IOException e) {
				logger.error("parserConfStr2Properties Error", e);
			} finally {
				if (byteArrayInputStream != null) {
					byteArrayInputStream.close();
				}
			}
		}
		return prop;
	}
}