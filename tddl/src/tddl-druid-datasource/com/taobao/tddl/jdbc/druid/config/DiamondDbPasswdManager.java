package com.taobao.tddl.jdbc.druid.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.TDDLConstant;
import com.taobao.tddl.common.config.ConfigDataHandler;
import com.taobao.tddl.common.config.ConfigDataHandlerFactory;
import com.taobao.tddl.common.config.ConfigDataListener;
import com.taobao.tddl.common.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.jdbc.druid.common.DruidConfParser;
import com.taobao.tddl.jdbc.druid.common.DruidConstants;

/**
 * 密码管理器Diamond实现
 * 
 * @author qihao
 * 
 */
public class DiamondDbPasswdManager implements DbPasswdManager {
	private static Log logger = LogFactory.getLog(DiamondDbPasswdManager.class);
	private String passwdConfDataId;
	private String unitName;
	private ConfigDataHandlerFactory configFactory;
	private ConfigDataHandler passwdHandler;
	private volatile List<ConfigDataListener> passwdConfListener = new ArrayList<ConfigDataListener>();

	public void init(String appName) {
		configFactory = ConfigDataHandlerCity.getFactory(appName);
		Map<String, String> config = new HashMap<String, String>();
		config.put("group", DruidConstants.DEFAULT_DIAMOND_GROUP);
		passwdHandler = configFactory.getConfigDataHandlerWithFullConfig(
				passwdConfDataId, passwdConfListener,
				Executors.newSingleThreadScheduledExecutor(), config, unitName);
	}

	public String getPasswd() {
		if (null != passwdHandler) {
			String passwdStr = passwdHandler.getData(
					TDDLConstant.DIAMOND_GET_DATA_TIMEOUT,
					ConfigDataHandler.FIRST_SERVER_STRATEGY);
			if (passwdStr == null) {
				logger.error("[getDataError] remote password string is empty !");
				return null;
			}
			return DruidConfParser.parserPasswd(passwdStr);
		}
		logger.error("[getDataError] passwdConfig not init !");
		return null;
	}

	public void registerPasswdConfListener(ConfigDataListener Listener) {
		passwdConfListener.add(Listener);
	}

	public void setPasswdConfDataId(String passwdConfDataId) {
		this.passwdConfDataId = passwdConfDataId;
	}
	
	public void setUnitName(String unitName) {
		this.unitName = unitName;
	}

	public void stopDbPasswdManager() {
		if (null != this.passwdHandler) {
			this.passwdHandler.closeUnderManager();
		}
	}
}
