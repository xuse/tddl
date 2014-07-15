package com.taobao.tddl.client.jdbc.replication;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.common.sync.BizTDDLContext;
import com.taobao.tddl.common.sync.SlaveInfo;
import com.taobao.tddl.common.sync.SlaveReplicater;

/**
 * 行复制辅助类 
 * @author linxuan
 */
public class ReplicationHelper {
	
	/**
	 * 用TDataSource中的master4replication/slave4replication
	 * 初始化复制配置结构BizTDDLContext中的主库读库数据源
	 * @param tds 应用的TDataSource数据源
	 * @param logicTableName2TDDLContext: key：逻辑表名；value：复制配置
	 */
	public static void initReplicationContextByTDataSource(TDataSource tds,
			Map<String, BizTDDLContext> logicTableName2TDDLContext) {

		for (Map.Entry<String, BizTDDLContext> e : logicTableName2TDDLContext.entrySet()) {
			BizTDDLContext rplCtx = e.getValue();
			rplCtx.setMasterJdbcTemplate(new JdbcTemplate(tds));
			for (SlaveInfo slaveInfo : rplCtx.getSlaveInfos()) {
				if (slaveInfo.getName() == null) {
					slaveInfo.setName(e.getKey());
				}
				if (slaveInfo.getSlaveReplicater() != null) {
					continue;
				}
				if (slaveInfo.getSlaveReplicaterName() != null) {
					slaveInfo.setSlaveReplicater((SlaveReplicater) tds.getSpringContext().getBean(
							slaveInfo.getSlaveReplicaterName()));
					continue;
				}
				if (slaveInfo.getJdbcTemplate() != null) {
					continue;
				}
				
				if (slaveInfo.getDataSourceName() == null) {
					throw new IllegalStateException("2.4.1之后SlaveInfo的 DataSourceName属性必须设置");
				} else {
					DataSource targetDS = tds.getReplicationTargetDataSources().get(slaveInfo.getDataSourceName());
					if (targetDS == null) {
						// 兼容旧实现：要以dataSourceName为key将复制的目标库配置到主TDataSource中的dataSourcePool中
						targetDS = tds.getDataSource(slaveInfo.getDataSourceName());
					}
					if (targetDS == null) {
						throw new IllegalArgumentException("[SlaveInfo.dataSourceName]Replication target DataSource"
								+ " could not found in TDataSource config:" + slaveInfo.getDataSourceName());
					}
					slaveInfo.setJdbcTemplate(new JdbcTemplate(targetDS));
				}
			}
		}
	}
}
