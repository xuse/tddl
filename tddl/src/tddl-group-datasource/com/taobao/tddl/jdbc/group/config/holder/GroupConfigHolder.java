package com.taobao.tddl.jdbc.group.config.holder;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.ConfigServerHelper;
import com.taobao.tddl.common.config.impl.AbstractConfigHolder;
import com.taobao.tddl.common.exception.MissingConfigException;
import com.taobao.tddl.common.util.StringUtils;
import com.taobao.tddl.jdbc.group.TGroupDataSource;

public class GroupConfigHolder extends AbstractConfigHolder {
	
	private static final Log log = LogFactory.getLog(GroupConfigHolder.class);
	
	private final String appName;

	private final List<String> groups;
	
	private final String unitName;
	
	private static final String ATOM_CONFIG_HOLDER_NAME = "com.taobao.tddl.common.config.impl.AtomConfigHolder";

	public GroupConfigHolder(String appName, List<String> groups,String unitName) {
		this.appName = appName;
		this.groups = groups;
		this.unitName = unitName;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void initSonHolder(List<String> atomKeys) throws Exception {
		Class sonHolderClass = Class.forName(ATOM_CONFIG_HOLDER_NAME);
		Constructor constructor = sonHolderClass.getConstructor(String.class, List.class, String.class);
		sonConfigDataHolder = (AbstractConfigHolder)constructor.newInstance(this.appName, atomKeys, this.unitName);
		sonConfigDataHolder.init();
	}

	public void init() {
		List<String> fullGroupKeys = getFullDbGroupKeys(groups);
		Map<String, String> queryResult = queryAndHold(fullGroupKeys,unitName);
		initExtraConfigs();
		

		if(queryResult == null || queryResult.size() == 0){
			log.error("Group batch get diamond config failed. get one by one.");
			return;
		}
		List<String> atomKeys = new ArrayList<String>();
		for(Entry<String,String> entry : queryResult.entrySet()){
			if(StringUtils.nullOrEmpty(entry.getValue())) {
				throw new MissingConfigException("Group Config Is Null, AppName >> " + appName + " ## UnitName >> " + unitName + " ## GroupKey >> " + entry.getKey());
			}
			String[] dsWeightArray = entry.getValue().split(",");
			for(String inValue : dsWeightArray){
				atomKeys.add(inValue.split(":")[0]);
			}
		}
		
		try {
			initSonHolder(atomKeys);
		} catch (Exception e) {
			throw new IllegalStateException("Init SonConfigHolder Error, Class is : " + ATOM_CONFIG_HOLDER_NAME, e);
		}
	}
	
	private void initExtraConfigs(){
		List<String> extraConfKeys = getExtraConfKeys(groups);
		extraConfKeys.add(ConfigServerHelper.getTddlConfigDataId(appName));
		queryAndHold(extraConfKeys,unitName);
	}
	
	private List<String> getExtraConfKeys(List<String> groupKeys) {
		List<String> result = new ArrayList<String>();
		for(String key : groupKeys){
			result.add(getExtraConfKey(key));
		}
		return result;
	}
	
	private String getExtraConfKey(String groupKey) {
		return TGroupDataSource.EXTRA_PREFIX + groupKey + "." + appName;
	}
	
	private List<String> getFullDbGroupKeys(List<String> groupKeys) {
		List<String> result = new ArrayList<String>();
		for(String key : groupKeys){
			result.add(getFullDbGroupKey(key));
		}
		return result;
	}
	
	private String getFullDbGroupKey(String groupKey) {
		return TGroupDataSource.PREFIX + groupKey;
	}

	public static void main(String[] args) {
		GroupConfigHolder holder = new GroupConfigHolder("JIECHEN_YUGONG_APP",
				Arrays.asList("YUGONG_TEST_APP_GROUP_1",
						"YUGONG_TEST_APP_GROUP_2"), null);
		holder.init();
		System.out.println("OUT");
	}
}
