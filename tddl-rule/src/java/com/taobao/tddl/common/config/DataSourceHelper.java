/*package com.taobao.tddl.common.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.taobao.datasource.LocalTxDataSourceDO;
import com.taobao.datasource.TaobaoDataSourceFactory;
import com.taobao.datasource.resource.adapter.jdbc.local.LocalTxDataSource;
import com.taobao.tddl.common.jdbc.DataSourceConfig;


 * @author guangxia
 * @since 1.0, 2009-12-14 下午04:35:24
 
public class DataSourceHelper {
		
	private static Log logger = LogFactory.getLog(DataSourceHelper.class);
	
	public static LocalTxDataSource findDataSource(DataSourceConfig config, Map<String, DataSourceConfig> dataSourceConfigs) {
		if(dataSourceConfigs == null) {
			return null;
		}
		//如果配置了JNDI这返回
		if("jndi".equalsIgnoreCase(config.getType())) {
			return null;
		}
		for (Entry<String,DataSourceConfig> entry : dataSourceConfigs.entrySet()) {
			DataSourceConfig dataSourecConfig=entry.getValue();
			LocalTxDataSourceDO oldConfig = dataSourecConfig.getDsConfig();
			//判断必要参数是否相当，如果这些必要的参数都相等则认为是同一个数据源
			LocalTxDataSourceDO	nowConfigDO=config.getDsConfig();
			if (StringUtil.equals(oldConfig.getConnectionURL(), nowConfigDO.getConnectionURL())
					&& StringUtil.equals(oldConfig.getDriverClass(), nowConfigDO.getDriverClass())
					&& StringUtil.equals(oldConfig.getUserName(), nowConfigDO.getUserName())
					&& StringUtil.equals(oldConfig.getPassword(), nowConfigDO.getPassword())
					&& ((oldConfig.getMaxPoolSize() == 0 && nowConfigDO.getMaxPoolSize() == 0) 
					|| (oldConfig.getMaxPoolSize()>0 && oldConfig.getMaxPoolSize()==nowConfigDO.getMaxPoolSize()))
					&& ((oldConfig.getMinPoolSize() == 0 && nowConfigDO.getMinPoolSize() == 0) 
					|| (oldConfig.getMinPoolSize()>0 && oldConfig.getMinPoolSize()==nowConfigDO.getMinPoolSize()))) {
				Object dataSource=dataSourecConfig.getDsObject();
				if(dataSource instanceof LocalTxDataSource){
					return (LocalTxDataSource) dataSource;
				}
			}
		}
		logger.error("Tddl Comparing Jboss DataSource Not Found! NewConf: "+config.getDsConfig());
		return null;
	}
	
	public static DataSource buildDataSource(DataSourceConfig config) {
		if("jndi".equalsIgnoreCase(config.getType())) {
			Context context;
			try {
				context = new InitialContext();
				return (DataSource) context.lookup(config.getDsConfig().getJndiName());
			} catch (NamingException e) {
				logger.error("Get datasource from jndi fail!", e);
				return null;
			}
		}
		DataSource dataSource=null;
		try {
			LocalTxDataSource localDataSource=TaobaoDataSourceFactory.createLocalTxDataSource(config.getDsConfig());
			config.setDsObject(localDataSource);
			dataSource=localDataSource.getDatasource();
			logger.error("Create New Jboss DataSource is : " +config.getDsConfig());
		} catch (Exception e) {
			logger.error("Create Jboss DataSource Error ! " +config.toString(), e);
		}
        return dataSource;
	}
	
	public static Map<String, DataSource> buildDataSources(Map<String, DataSourceConfig> configs){
		Map<String, DataSource> dataSources = new HashMap<String, DataSource>(configs.size());
		for(Entry<String, DataSourceConfig> e : configs.entrySet()) {
			DataSourceConfig config=e.getValue();
			dataSources.put(e.getKey(), buildDataSource(config));
		}
		return dataSources;
	}

	public static Map<String, DataSource> buildDataSource(Map<String, DataSourceConfig> configs, Map<String, DataSourceConfig> oldConfigs) {
		Map<String, DataSource> newDataSources = new HashMap<String, DataSource>(configs.size());
		List<LocalTxDataSource> bothInNewAndOld = new ArrayList<LocalTxDataSource>();
		for(Entry<String, DataSourceConfig> entry : configs.entrySet()) {
			DataSourceConfig config = entry.getValue();
			LocalTxDataSource  localTxDataSource = findDataSource(config, oldConfigs);
			if (localTxDataSource != null) {
				newDataSources.put(entry.getKey(), localTxDataSource.getDatasource());
				bothInNewAndOld.add(localTxDataSource);
			} else {
				newDataSources.put(entry.getKey(), buildDataSource(config));
			}
		}
		if (null != oldConfigs) {
			// 将oldConfigs转换成List<LocalTxDataSource>
			List<LocalTxDataSource> oldDataSourceList = new ArrayList<LocalTxDataSource>(
					oldConfigs.size());
			for (DataSourceConfig oldDataSourceConfig : oldConfigs.values()) {
				Object oldDataSource = oldDataSourceConfig.getDsObject();
				if(!(oldDataSource instanceof LocalTxDataSource)){
					continue;
				}
				if (null != oldDataSource) {
					oldDataSourceList.add((LocalTxDataSource) oldDataSource);
				}
			}
			for (LocalTxDataSource oldLocalTxDataSource : oldDataSourceList) {
				if (!bothInNewAndOld.contains(oldLocalTxDataSource)) {
					try {
						oldLocalTxDataSource.destroy();
						logger.debug("Old LocalTxDataSource close success: "+ oldLocalTxDataSource);
					} catch (Exception e) {
						logger.info("Old LocalTxDataSource close fail: "+ oldLocalTxDataSource, e);
					}
				}
			}
		}
		return newDataSources;
	}
}
*/