package com.taobao.tddl.common.util;

import javax.sql.DataSource;

import com.taobao.tddl.interact.rule.bean.DBType;

/**
 * 为了避免对TGroupDataSource这一层对spring的依赖
 * 
 * @author linxuan
 * 
 */
public interface DataSourceFetcher {
	DataSource getDataSource(String key);
	DBType getDataSourceDBType(String key);
}
