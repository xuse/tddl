package com.taobao.tddl.common.config.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import com.taobao.tddl.common.config.ConfigDataHandler;
import com.taobao.tddl.common.config.ConfigDataListener;
import com.taobao.tddl.common.config.diamond.PreHeatDataHandler;

/**
 * @author JIECHEN
 *
 */
public class PreHeatDataHandlerFactory extends DefaultConfigDataHandlerFactory {
	private String appName;
	
	public PreHeatDataHandlerFactory(String appName) {
		this.appName = appName;
	}
	
	@Override
	public ConfigDataHandler getConfigDataHandlerWithFullConfig(String dataId,
			List<ConfigDataListener> configDataListenerList, Executor executor,
			Map<String, String> config, String unitName) {
		Map<String, Object> configMap = getConfigMap(config);
		ConfigDataHandler instance = new PreHeatDataHandler(appName);
		if (ConfigHolderFactory.isInit(appName)) {
			String initialData = ConfigHolderFactory.getConfigDataHolder(
					appName).getData(dataId);
			instance.init(dataId, clearNullListener(configDataListenerList), configMap, unitName,
					initialData);
		} else {
			instance.init(dataId, clearNullListener(configDataListenerList), configMap, unitName);
		}
		return instance;
	}
}
 