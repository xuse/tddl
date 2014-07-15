package com.taobao.tddl.common.config.impl;

import com.taobao.tddl.common.config.ConfigDataHandlerFactory;

public class ConfigDataHandlerCity {
	
	public static ConfigDataHandlerFactory getFactory(String appName){
		if(appName == null || appName.trim().isEmpty()){
			return getSimpleFactory();
		}
		return getPreHeatFactory(appName);
	}
	
	public static DefaultConfigDataHandlerFactory getSimpleFactory(){
		return new DefaultConfigDataHandlerFactory();
	}
	
	public static PreHeatDataHandlerFactory getPreHeatFactory(String appName){
		return new PreHeatDataHandlerFactory(appName);
	}

}
