package com.taobao.tddl.rule.ruleengine.entities.convientobjectmaker;

import java.util.Map;

import com.taobao.tddl.rule.ruleengine.entities.abstractentities.SharedElement;

/**
 * 
 * 用于创建database中持有多个表的map
 * 
 * @author shenxun
 *
 */
public interface TableMapProvider {
	public Map<String, SharedElement> getTablesMap();
	public void setParentID(String parentID) ;
	public void setLogicTable(String logicTable);
}
