package com.taobao.tddl.rule.bean;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.rule.ruleengine.entities.abstractentities.SharedElement;
import com.taobao.tddl.rule.ruleengine.entities.convientobjectmaker.TableMapProvider;

public class UppSpecTableMapProvider implements TableMapProvider{

	private String logicTable;
	private String tabPreffix = "upp";
	private String padding = "_";
	public Map<String, SharedElement> getTablesMap() {
		Table table = new Table();
		if(logicTable == null){
			throw new IllegalArgumentException("没有表名生成因子");
		}
		table.setTableName(tabPreffix + padding + logicTable);
		Map<String, SharedElement> returnMap = new HashMap<String, SharedElement>();
		returnMap.put("0", table);
		return returnMap;
	}

	public void setParentID(String parentID) {
		//do nothing
	}

	
	public String getTabPreffix() {
		return tabPreffix;
	}

	public void setTabPreffix(String tabPreffix) {
		this.tabPreffix = tabPreffix;
	}

	public String getPadding() {
		return padding;
	}

	public void setPadding(String padding) {
		this.padding = padding;
	}

	public String getLogicTable() {
		return logicTable;
	}

	public void setLogicTable(String logicTable) {
		this.logicTable = logicTable;
	}

}
