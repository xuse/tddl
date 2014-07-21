package com.taobao.tddl.common.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import com.taobao.tddl.common.datasource.LocalTxDataSourceDO;

public class DataSourceConfig implements Cloneable {

	/**
	 * 数据源的信息DO
	 */
	private LocalTxDataSourceDO dsConfig;

	private boolean isLocalTxDataSource;
	/**
	 * 数据源对象，可能是直接实现DataSource的对象，也可能是剥离JBOOS数据源的LocalTxDataSource对象
	 */
	private Object dsObject;

	/**
	 * 类型 jndi或者其他
	 */
	private String type;

	private boolean alive = true;

	public DataSourceConfig() {

	}

	public DataSourceConfig(LocalTxDataSourceDO dsConfig, Object dsObject,
			String type) {
		this.dsConfig = dsConfig;
		this.dsObject = dsObject;
		this.type = type;
	}

	public LocalTxDataSourceDO getDsConfig() {
		return dsConfig;
	}

	public void setDsConfig(LocalTxDataSourceDO dsConfig) {
		this.dsConfig = dsConfig;
	}

	public Object getDsObject() {
		return dsObject;
	}

	public void setDsObject(Object dsObject) {
		this.dsObject = dsObject;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public boolean isContainsDsObj() {
		return null != dsObject;
	}

	public boolean isAlive() {
		return alive;
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	public DataSourceConfig clone() {
		DataSourceConfig dsConfig = new DataSourceConfig();
		LocalTxDataSourceDO config = null;
		if (null != this.getDsConfig()) {
			config = this.getDsConfig().clone();
			dsConfig.setDsConfig(config);
		}
		dsConfig.setAlive(this.alive);
		dsConfig.setType(this.getType());
		return dsConfig;
	}

	public DataSource getDataSource() {
		DataSource dataSource = null;
		if (this.dsObject instanceof DataSource) {
			dataSource = (DataSource) this.dsObject;
		} 
		else {
			dataSource = getTbDatasource();
		}
		return dataSource;
	}

	private DataSource getTbDatasource() {
		if (isTbDatasource == null) {
			DataSource result = null;
			try {
				Object dsObject = dsClass.cast(this.dsObject);
				result = (DataSource)getDsMethod.invoke(dsObject);
				isTbDatasource = Boolean.TRUE;
			} catch (Exception e) {
				isTbDatasource = Boolean.FALSE;
			}
			return result;
		}

		if (isTbDatasource.booleanValue()) {
			try {
				Object dsObject = dsClass.cast(this.dsObject);
				return  (DataSource)getDsMethod.invoke(dsObject);
			} catch (Exception e) {
			}
		}

		return null;

	}
	private static Boolean isTbDatasource;

	private static String className = "com.taobao.datasource.resource.adapter.jdbc.local.LocalTxDataSource";
	private static Class dsClass = null;
	private static Method getDsMethod = null;
	static {
		try {
			dsClass = Class.forName(className);
			getDsMethod = dsClass.getDeclaredMethod("getDatasource");
		} catch (Exception e) {
			isTbDatasource = Boolean.FALSE;
		}
	}

}
