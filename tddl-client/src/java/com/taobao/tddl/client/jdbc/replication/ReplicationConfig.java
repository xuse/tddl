package com.taobao.tddl.client.jdbc.replication;

import java.util.Map;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.common.ConfigServerHelper;
import com.taobao.tddl.common.ConfigServerHelper.DataListener;
import com.taobao.tddl.common.config.DefaultTddlConfigParser;
import com.taobao.tddl.common.config.TddlConfigParser;
import com.taobao.tddl.common.sync.BizTDDLContext;

/**
 * 行复制上下文环境
 * 包含行复制需要的所有配置：日志库配置、如何复制的配置
 * 实时行复制和补偿服务器共用
 * 
 * @author linxuan
 *
 */
public class ReplicationConfig {
	//private EquityDbManager syncLogDb;
	private boolean isUseLocalConfig = false;
	private String appName;
	private Map<String, BizTDDLContext> logicTableName2TDDLContext;
	private String replicationConfigFile; //新本地文件配置

	private TddlConfigParser<Map<String, BizTDDLContext>> configParser = new DefaultTddlConfigParser<Map<String, BizTDDLContext>>();
	private final DataListener replicationListener = new DataListener() {
		public void onDataReceive(Object data) {
			//不支持动态修改
		}

		public void onDataReceiveAtRegister(Object data) {
			if (data != null) {
				//TODO 用runtimeHolder包起来
				ReplicationConfig.this.logicTableName2TDDLContext = configParser.parseCongfig((String) data);
			}
		}
	};

	public void init(TDataSource tds) {
		if (!this.isUseLocalConfig) {
			this.logicTableName2TDDLContext = null;
			//订阅行复制配置
			Object first = ConfigServerHelper.subscribeReplicationConfig(this.appName, replicationListener);
			if (first == null) {
				throw new IllegalStateException("没有接收到行复制配置");
			}
			if (this.logicTableName2TDDLContext == null) {
				throw new IllegalStateException("解析行复制配置失败：" + first);
			}
		} else if (replicationConfigFile != null) {
			FileSystemXmlApplicationContext ctx = new FileSystemXmlApplicationContext(replicationConfigFile);
			this.logicTableName2TDDLContext = convert(ctx.getBean("root"));
		} else if (this.logicTableName2TDDLContext == null) {
			throw new IllegalArgumentException("logicTableName2TDDLContext属性没有配置");
		}

		//用TDataSource初始化logicTableName2TDDLContext
		ReplicationHelper.initReplicationContextByTDataSource(tds, this.logicTableName2TDDLContext);
	}

	@SuppressWarnings("unchecked")
	private <T> T convert(Object obj) {
		return (T) obj;
	}

	/**
	 * 无逻辑的getter/setter
	 */
	/*public EquityDbManager getSyncLogDb() {
		return syncLogDb;
	}

	public void setSyncLogDb(EquityDbManager syncLogDb) {
		this.syncLogDb = syncLogDb;
	}*/

	public void setLogicTableName2TDDLContext(Map<String, BizTDDLContext> logicTableName2TDDLContext) {
		this.logicTableName2TDDLContext = logicTableName2TDDLContext;
	}

	public Map<String, BizTDDLContext> getLogicTableName2TDDLContext() {
		return logicTableName2TDDLContext;
	}

	public void setUseLocalConfig(boolean isUseLocalConfig) {
		this.isUseLocalConfig = isUseLocalConfig;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public void setReplicationConfigFile(String replicationConfigFile) {
		this.replicationConfigFile = replicationConfigFile;
	}
}
