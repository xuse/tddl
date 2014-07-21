package com.taobao.tddl.common.config.beans;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.taobao.tddl.rule.bean.TDDLRoot;



/**
 * 一份完整的读写分离和分库分表规则配置
 * 一个业务一份
 *  
 * @author linxuan
 * 去掉了master slave的逻辑，并作兼容性调整
 * @author shenxun 
 */
public class AppRule{
	private TDDLRoot defaultTddlRoot = null;
	private Map<String,TDDLRoot> rootMap = new HashMap<String, TDDLRoot>(4);
	
	public void init() {
		defaultTddlRoot.init();
		for(Entry<String, TDDLRoot> entry : rootMap.entrySet()){
			entry.getValue().init();
		}
	}
	
	private void setDefaultRoot(TDDLRoot tddlRoot ){
		if(this.defaultTddlRoot != null && tddlRoot != null){
			throw new IllegalArgumentException("rw master rule .master rule与slave rule不可同时配置");
		}
		this.defaultTddlRoot = tddlRoot; 
	}

	public TDDLRoot getMasterRule() {
		return defaultTddlRoot;
	}

	public void setMasterRule(TDDLRoot masterRule) {
		setDefaultRoot(masterRule);
		rootMap.put("MASTER", masterRule);
	}

	public TDDLRoot getSlaveRule() {
		return defaultTddlRoot;
	}

	public void setSlaveRule(TDDLRoot slaveRule) {
		rootMap.put("SLAVE", slaveRule);
	}

	public TDDLRoot getReadwriteRule() {
		return defaultTddlRoot;
	}
	
	public TDDLRoot getDefaultTddlRoot() {
		return defaultTddlRoot;
	}
	
	public void setDefaultTddlRoot(TDDLRoot defaultTddlRoot) {
		this.defaultTddlRoot = defaultTddlRoot;
	}
	
	public void setReadwriteRule(TDDLRoot readwriteRule) {
		setDefaultRoot(readwriteRule);
	}
	
	public Map<String, TDDLRoot> getRootMap() {
		return rootMap;
	}
	
	public void setRootMap(Map<String, TDDLRoot> rootMap) {
		this.rootMap = rootMap;
	}
}
