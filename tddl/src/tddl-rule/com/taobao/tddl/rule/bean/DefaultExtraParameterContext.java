package com.taobao.tddl.rule.bean;

import java.util.Map;

import com.taobao.tddl.interact.rule.bean.ExtraParameterContext;

/**
 * ExtraParameterContext的一个默认实现类
 * 里面包含两个Map<Object,Object>对象，分别是dbMap,tabMap
 * 可分别用来存储db的相关的参数
 * 和tab的相关的参数
 * 
 * @author xudanhui.pt 2010-10-18,上午11:33:44
 */
public class DefaultExtraParameterContext implements ExtraParameterContext {

	private Map<Object, Object> dbMap;

	private Map<Object, Object> tabMap;

	public Map<Object, Object> getDbMap() {
		return dbMap;
	}

	public void setDbMap(Map<Object, Object> dbMap) {
		this.dbMap = dbMap;
	}

	public Map<Object, Object> getTabMap() {
		return tabMap;
	}

	public void setTabMap(Map<Object, Object> tabMap) {
		this.tabMap = tabMap;
	}
}
