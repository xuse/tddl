package com.taobao.tddl.common.config.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.config.atom.TAtomConfConstants;
import com.taobao.tddl.common.config.atom.TAtomConfHelper;
import com.taobao.tddl.common.exception.MissingConfigException;
import com.taobao.tddl.common.util.StringUtils;

public class AtomConfigHolder extends AbstractConfigHolder {
	
	private static final Log log = LogFactory.getLog(AtomConfigHolder.class);
	
	private final String appName;

	private final List<String> atomKeys;
	
	private final String unitName;

	public AtomConfigHolder(String appName, List<String> atomKeys,String unitName) {
		this.appName = appName;
		this.atomKeys = atomKeys;
		this.unitName = unitName;
	}

	public void init() {
		Map<String, String> fullGlobalKeys = getFullGlobalKeyMap(atomKeys);
		Map<String, String> globalResults = queryAndHold(values2List(fullGlobalKeys),unitName);
		if(globalResults == null || globalResults.size() == 0){
			log.error("Atom batch get diamond global config failed. get one by one.");
			return;
		}
		
		Map<String, String> fullAppKeys = getFullAppKeyMap(atomKeys);
		Map<String, String> appKeyResults = queryAndHold(values2List(fullAppKeys),unitName);
		if(appKeyResults == null || appKeyResults.size() == 0){
			log.error("Group batch get diamond app config failed. get one by one.");
			return;
		}
		
		List<String> passWdKeys = new ArrayList<String>();
		for(String atomKey : atomKeys) {
			String globalValue = globalResults.get(fullGlobalKeys.get(atomKey));
			if(StringUtils.nullOrEmpty(globalValue)) {
				throw new MissingConfigException("Global Config Is Null, AppName >> " + appName + " ## UnitName >> " + unitName + " ## AtomKey >> " + atomKey);
			}
			Properties globalProperties = TAtomConfHelper.parserConfStr2Properties(globalValue);
			String dbName = globalProperties.getProperty(TAtomConfHelper.GLOBA_DB_NAME_KEY);
			String dbType = globalProperties.getProperty(TAtomConfHelper.GLOBA_DB_TYPE_KEY);
			
			String appValue = appKeyResults.get(fullAppKeys.get(atomKey));
			if(StringUtils.nullOrEmpty(appValue)) {
				throw new MissingConfigException("App Config Is Null, AppName >> " + appName + " ## UnitName >> " + unitName + " ## AtomKey >> " + atomKey);
			}
			Properties dbKeyProperties = TAtomConfHelper.parserConfStr2Properties(appValue);
			String userName = dbKeyProperties.getProperty(TAtomConfHelper.APP_USER_NAME_KEY);
			
			passWdKeys.add(getPassWdKey(dbName, dbType, userName));
		}
		queryAndHold(passWdKeys,unitName);
	}
	

	
	private List<String> values2List(Map<String, String> map){
		List<String> result = new ArrayList<String>();
		for(String string : map.values())result.add(string);
		return result;
	}
	
	private Map<String, String> getFullGlobalKeyMap(List<String> atomKeys){
		Map<String, String> result = new HashMap<String, String>();
		for(String atomKey : atomKeys) {
			result.put(atomKey, TAtomConfConstants.getGlobalDataId(atomKey));
		}
		return result;
	}
	
	private Map<String, String> getFullAppKeyMap(List<String> atomKeys){
		Map<String, String> result = new HashMap<String, String>();
		for(String atomKey : atomKeys) {
			result.put(atomKey, TAtomConfConstants.getAppDataId(appName, atomKey));
		}
		return result;
	}
	
	private String getPassWdKey(String dbName, String dbType, String userName) {
		return TAtomConfConstants.getPasswdDataId(dbName, dbType, userName);
	}
	
	public static void main(String[] args) {
		AtomConfigHolder holder = new AtomConfigHolder("JIECHEN_YUGONG_APP",
				Arrays.asList("yugong_test_1", "yugong_test_2"), null);
		holder.init();
		System.out.println("OUT");
	}
}
