package com.taobao.tddl.sqlobjecttree;

import java.util.Map;

import com.taobao.tddl.interact.sqljep.Comparative;

/*
 * @author guangxia
 * @since 1.0, 2009-10-22 ÏÂÎç02:57:32
 */
public class ColumnValuePairAndMatchRulesIndex {
	
	public static final int NO_INDEX = -1;
	
	private int uniqueKeyIndex = NO_INDEX;
	private int tableShardingIndex = NO_INDEX;
	private int databaseSharedingIndex = NO_INDEX;
	private Map<String, Comparative> uniqueKeyColumnsMap;
	private Map<String, Comparative> tableShardingColumnsMap;
	private Map<String, Comparative> databaseSharedingColumnsMap;
	
	public void setUniqueKeyIndex(int uniqueKeyIndex) {
		this.uniqueKeyIndex = uniqueKeyIndex;
	}
	public int getUniqueKeyIndex() {
		return uniqueKeyIndex;
	}
	public void setTableShardingIndex(int tableShardingIndex) {
		this.tableShardingIndex = tableShardingIndex;
	}
	public int getTableShardingIndex() {
		return tableShardingIndex;
	}
	public void setDatabaseSharedingIndex(int databaseSharedingIndex) {
		this.databaseSharedingIndex = databaseSharedingIndex;
	}
	public int getDatabaseSharedingIndex() {
		return databaseSharedingIndex;
	}
	public void setUniqueKeyColumnsMap(Map<String, Comparative> uniqueKeyColumnsMap) {
		this.uniqueKeyColumnsMap = uniqueKeyColumnsMap;
	}
	public Map<String, Comparative> getUniqueKeyColumnsMap() {
		return uniqueKeyColumnsMap;
	}
	public void setTableShardingColumnsMap(Map<String, Comparative> tableShardingColumnsMap) {
		this.tableShardingColumnsMap = tableShardingColumnsMap;
	}
	public Map<String, Comparative> getTableShardingColumnsMap() {
		return tableShardingColumnsMap;
	}
	public void setDatabaseSharedingColumnsMap(
			Map<String, Comparative> databaseSharedingColumnsMap) {
		this.databaseSharedingColumnsMap = databaseSharedingColumnsMap;
	}
	public Map<String, Comparative> getDatabaseSharedingColumnsMap() {
		return databaseSharedingColumnsMap;
	}

}
