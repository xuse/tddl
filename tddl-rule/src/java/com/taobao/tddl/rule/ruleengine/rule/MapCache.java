package com.taobao.tddl.rule.ruleengine.rule;

import java.util.Map;

import javax.sql.DataSource;

/**
 * TODO:小心tair有可能的并发问题
 * @author shenxun
 *
 */
public interface MapCache {
	/**
	 * 放入
	 * @param nameSpace 对应表名，也对应cache中的namespace
	 * @param values
	 * @return affect rows
	 */
	public int put(String nameSpace ,Map<String, Object> values);
	/**
	 * 拿出
	 * @param nameSpace 对应表名，也对应cache中的namespace
	 * @param key 对应key
	 * @param column 该value在sql中的列名，key的列名是和dba进行约定就可以解决的
	 * @return
	 */
	public Object get(String nameSpace ,Object key,String column);
	public void setTargetDatasource(DataSource targetDatasource) ;
}
