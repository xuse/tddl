package com.taobao.tddl.client.jdbc.listener;

import java.sql.SQLException;

import com.taobao.tddl.client.jdbc.TDataSource;

/*
 * @author guangxia
 * @since 1.0, 2010-4-13 下午02:34:56
 */
public interface Adapter {
	
	void init(TDataSource tDataSource, HookPoints hookPoints) throws SQLException;

}
