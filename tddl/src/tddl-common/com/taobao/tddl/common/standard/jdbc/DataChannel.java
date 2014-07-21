package com.taobao.tddl.common.standard.jdbc;

import com.taobao.tddl.common.channel.SqlMetaData;

/**
 * @author JIECHEN
 */
public interface DataChannel{
	
	/**
	 * 传递该sql的元信息给底层
	 * @param sqlMetaData
	 */
	public void fillMetaData(SqlMetaData sqlMetaData);
	
	public SqlMetaData getSqlMetaData();
	
}
