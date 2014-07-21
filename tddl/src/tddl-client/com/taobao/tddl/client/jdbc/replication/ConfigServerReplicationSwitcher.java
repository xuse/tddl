package com.taobao.tddl.client.jdbc.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.io.ByteArrayInputStream;
import com.taobao.tddl.common.ConfigServerHelper;
import com.taobao.tddl.common.ConfigServerHelper.AbstractDataListener;
import com.taobao.tddl.common.ConfigServerHelper.DataListener;

/**
 * 基于configServer的ReplicationSwitcher实现
 * 
 * dataId: com.taobao.tddl.ConfigServerReplicationSwitcher.configProperties
 * group:由业务通过属性configServiceGroup设置
 * 
 * 推送来的数据，是一个Properties对象。其中的属性包括：
 * stopAllReplication=false
 * 
 * @author linxuan
 */
public class ConfigServerReplicationSwitcher implements ReplicationSwitcher {
	private static final Log logger = LogFactory.getLog(ConfigServerReplicationSwitcher.class);
	private String appName;
	private volatile Level level = Level.ALL_ON;
	private volatile InsertSyncLogMode insertSyncLogMode = InsertSyncLogMode.normal;
	private List<ReplicationConfigAware> replicationConfigAwares = new ArrayList<ReplicationConfigAware>(1);

	private final DataListener replicationSwitchListener = new AbstractDataListener() {
		public void onDataReceive(Object data) {
			parsePropertiesConfig(ConfigServerHelper.parseProperties(data, "[replicationSwitchListener]"));
		}
	};

	public ConfigServerReplicationSwitcher() {
	}

	/**
	 * 使用方最好保证单例
	 */
	public void init() {
		//订阅行复制开关配置
		Object firstFetchedConfigs = ConfigServerHelper.subscribeReplicationSwitch(this.appName,
				replicationSwitchListener);
		if (firstFetchedConfigs == null) {
			if (level == null) {
				throw new IllegalArgumentException("既不能从config server获得行复制开关配置，也没有默认配置!");
			} else {
				logger.warn("使用本地level配置：" + this.level);
			}
		}
	}

	public Level level() {
		return this.level;
	}
	public InsertSyncLogMode insertSyncLogMode() {
		return this.insertSyncLogMode;
	}

	public void addReplicationConfigAware(ReplicationConfigAware replicationConfigAware) {
		this.replicationConfigAwares.add(replicationConfigAware);
	}


	/**
	 * @param properties 样例如下：
	 * stopAllReplication=false
	 */
	private void parsePropertiesConfig(Properties properties) {
		if (properties == null) {
			logger.warn("Empty ReplicationSwitcher config");
			return;
		}
		PropKey propKey;
		for (Entry<Object, Object> entry : properties.entrySet()) {
			String key = ((String) entry.getKey()).trim();
			String value = ((String) entry.getValue()).trim();
			try {
				propKey = PropKey.valueOf(key);
			} catch (Exception e) {
				logger.error("Failed to parse ReplicationSwitcher's properties key:" + key, e);
				continue;
			}
			switch (propKey) {
			case level:
				StringBuilder sb1 = new StringBuilder("Old level [").append(this.level);
				sb1.append("] switching to new level [").append(value).append("] ");
				try {
					this.level = Level.valueOf(value);
					sb1.append("succeed.");
				} catch (Exception e) {
					sb1.append("failed:").append(e.getMessage());
				}
				logger.warn(sb1.toString());
				break;
			case insertSyncLogMode:
				StringBuilder sb = new StringBuilder("Old insertSyncLogMode [").append(this.insertSyncLogMode);
				sb.append("] switching to new insertSyncLogMode [").append(value).append("] ");
				try {
					this.insertSyncLogMode = InsertSyncLogMode.valueOf(value);
					sb.append("succeed.");
				} catch (Exception e) {
					sb.append("failed:").append(e.getMessage());
				}
				logger.warn(sb.toString());
				break;
			case replicationThreadPoolSize:
				if (!replicationConfigAwares.isEmpty()) {
					Integer poolsize = null;
					try {
						poolsize = Integer.valueOf(value);
					} catch (Exception e) {
						logger.error("Failed to parse replicationThreadPoolSize:" + value, e);
					}
					if (poolsize != null) {
						for (ReplicationConfigAware aware : replicationConfigAwares) {
							aware.setReplicationThreadPoolSize(poolsize);
						}
					}
				}
				break;
			case insertSyncLogThreadPoolSize:
				if (!replicationConfigAwares.isEmpty()) {
					Integer poolsize = null;
					try {
						poolsize = Integer.valueOf(value);
					} catch (Exception e) {
						logger.error("Failed to parse insertSyncLogThreadPoolSize:" + value, e);
					}
					if (poolsize != null) {
						for (ReplicationConfigAware aware : replicationConfigAwares) {
							aware.setInsertSyncLogThreadPoolSize(poolsize);
						}
					}
				}
				break;
			default:
				throw new IllegalStateException("PropKey 增加了选项却没有在这里更新");
				//...其他属性这里扩展
			}
		}
	}

	public void setLevel(String level) {
		this.level = Level.valueOf(level);
	}

	public void setInsertSyncLogMode(String insertSyncLogMode) {
		this.insertSyncLogMode = InsertSyncLogMode.valueOf(insertSyncLogMode);
	}

	/**
	 * 无逻辑getter/setter
	 */
	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getAppName() {
		return appName;
	}

	public static void main(String[] args) {
		String str = "level=ALL_OFF";
		Properties p = new Properties();
		try {
			p.load(new ByteArrayInputStream(str.getBytes()));
		} catch (IOException e) {
			logger.error("无法解析推送的配置：" + str, e);
			return;
		}
		System.out.println(p.get("level"));
		System.out.println(Level.valueOf("bbb"));
	}
}
