package com.taobao.tddl.common.sync;

import java.util.Map;

/**
 * 外挂数据复制（同步）接口。回调接口
 * 
 * @author linxuan
 * 
 */
public interface SlaveReplicater {
	
	/**
	 * 在主库插入成功后调用
	 * @param masterRow 代表主库的一行数据。key：类名；value：列值
	 * @param slave 对应TDataSource.replicationConfigFile指向的复制配置文件(例如tddl-replication.xml)中的slaveInfo配置信息
	 */
	void insertSlaveRow(Map<String, Object> masterRow, SlaveInfo slave);

	/**
	 * 在主库更新成功后调用
	 * @param masterRow 代表主库的一行数据。key：类名；value：列值
	 * @param slave 对应TDataSource.replicationConfigFile指向的复制配置文件(例如tddl-replication.xml)中的slaveInfo配置信息
	 */
	void updateSlaveRow(Map<String, Object> masterRow, SlaveInfo slave);
}
