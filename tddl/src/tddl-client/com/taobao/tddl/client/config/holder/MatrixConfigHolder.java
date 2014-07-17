package com.taobao.tddl.client.config.holder;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.jdbc.sqlexecutor.parallel.ParallelDiamondConfigManager;
import com.taobao.tddl.common.ConfigServerHelper;
import com.taobao.tddl.common.config.impl.AbstractConfigHolder;
import com.taobao.tddl.common.exception.MissingConfigException;
import com.taobao.tddl.common.util.StringUtils;

public class MatrixConfigHolder extends AbstractConfigHolder {
	
	private static final Log log = LogFactory.getLog(MatrixConfigHolder.class);

	private final String appName;

	private final String unitName;
	
	private static final String GROUP_CONFIG_HOLDER_NAME = "com.taobao.tddl.jdbc.group.config.holder.GroupConfigHolder";

	public MatrixConfigHolder(String appName, String unitName) {
		this.appName = appName;
		this.unitName = unitName;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void initSonHolder(String dataId) throws Exception {
		String groupsValue = configHouse.get(dataId);
		if(StringUtils.nullOrEmpty(groupsValue)) {
			throw new MissingConfigException("Matrix Config Is Null, AppName >> " + appName + " ## UnitName >> " + unitName);
		}
		String[] groups = propertiesSpliter(groupsValue);
		Class sonHolderClass = Class.forName(GROUP_CONFIG_HOLDER_NAME);
		Constructor constructor = sonHolderClass.getConstructor(String.class, List.class, String.class);
		sonConfigDataHolder = (AbstractConfigHolder)constructor.newInstance(this.appName, Arrays.asList(groups), this.unitName);
		sonConfigDataHolder.init();
	}

	public void init() {
		String dataId = ConfigServerHelper.getDBGroupsConfig(this.appName);
		String sqlExcutorKey = ParallelDiamondConfigManager.getSqlExecutorKey(this.appName);
		Map<String, String> queryResult = queryAndHold(Arrays.asList(dataId, sqlExcutorKey),this.unitName);
		if(queryResult == null || queryResult.size() == 0){
			log.error("matrix batch get diamond config failed. get one by one");
			return;
		}
		try {
			initSonHolder(dataId);
		} catch (Exception e) {
			throw new IllegalStateException("Init SonConfigHolder Error, Class is : " + GROUP_CONFIG_HOLDER_NAME, e);
		}
	}
	
	/**
	 * groups«–∑÷
	 * @param target
	 * @return
	 */
	public String[] propertiesSpliter(String target) {
		if (target == null) {
			return null;
		}
		String[] tokens = target.split(",");
		for (int i = 0; i < tokens.length; i++) {
			tokens[i] = tokens[i].trim();
		}
		return tokens;
	}
	
	public static void main(String[] args) {
		MatrixConfigHolder holder = new MatrixConfigHolder("JIECHEN_YUGONG_APP",null);
		holder.init();
		System.out.println("OUT");
	}
}
