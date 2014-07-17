package com.taobao.tddl.client.jdbc;

import java.util.Collections;
import java.util.Map;

import javax.sql.DataSource;

/**
 * 保持为Final类，只可重建，不可修改
 * 
 * 第一阶段：只支持数据源的动态，不支持规则和dbindex的动态
 * 
 * @author linxuan
 * 
 */
public class TddlRuntime {
	public final Map<String, DataSource> dsMap;

	public TddlRuntime(Map<String, DataSource> datasourceMap) {
		this.dsMap = Collections.unmodifiableMap(datasourceMap);
	}
}
