package com.taobao.tddl.common;

import java.util.Map;

import javax.sql.DataSource;

public interface DataSourceChangeListener {
	public void onDataSourceChanged(Map<String, DataSource> dataSources);
}
