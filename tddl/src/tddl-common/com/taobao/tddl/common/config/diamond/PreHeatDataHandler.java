package com.taobao.tddl.common.config.diamond;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.config.impl.ConfigHolderFactory;
import com.taobao.tddl.common.util.StringUtils;

/**
 * @author JIECHEN
 *
 */
public class PreHeatDataHandler extends DiamondConfigDataHandler {
	private static final Log logger = LogFactory
			.getLog(PreHeatDataHandler.class);
	
	private String appName;
	
	public PreHeatDataHandler(String appName) {
		this.appName = appName;
	}
	
	public String getData(long timeout, String strategy) {
		if(ConfigHolderFactory.isInit(appName)){
			String result = ConfigHolderFactory.getConfigDataHolder(appName).getData(dataId);
			if(!StringUtils.nullOrEmpty(result)){
				return result;
			}
			logger.error("PreHeatDataHandler Miss Data, Use Default Handler. DataId Is : " +  dataId);
		}
		return super.getData(timeout, strategy);
	}
	
	public String getNullableData(long timeout, String strategy) {
		if(ConfigHolderFactory.isInit(appName)){
			return ConfigHolderFactory.getConfigDataHolder(appName).getData(dataId);
		}
		return super.getData(timeout, strategy);
	}
	
}
