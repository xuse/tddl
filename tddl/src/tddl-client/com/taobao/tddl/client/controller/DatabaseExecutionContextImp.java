package com.taobao.tddl.client.controller;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.interact.bean.Field;
import com.taobao.tddl.interact.bean.ReverseOutput;
import com.taobao.tddl.interact.bean.TargetDB;

public class DatabaseExecutionContextImp implements DatabaseExecutionContext
{
	public DatabaseExecutionContextImp()
	{
		tableNames = new LinkedList<Map<String, String>>();
	}

	/**
	 * 这个库在TDatasource索引中的索引
	 */
	private String dbIndex;
	
	private Map<String, Field> realTableFieldMap;

	/**
	 * 这个规则下的符合查询条件的表名列表
	 */
	private final List/*多个sql*/</*每一个sql内需要替换的表名*/Map<String/* logic table name */, String/* real table name */>> tableNames;
	
	/**
	 * 反向输出的sql,如果reverseOutput不为false,则这里不会为null. 但仍然可能为一个empty list
	 */
	private List<ReverseOutput> outputSQL;

	public String getDbIndex()
	{
		return dbIndex;
	}

	public void setDbIndex(String dbIndex)
	{
		this.dbIndex = dbIndex;
	}

	public List<Map<String, String>> getTableNames()
	{
		return tableNames;
	}

	/**
	 * 添加一个 表名对
	 * 
	 * Map<String 源表名, String 目标表名>
	 * @param pair
	 */
	public void addTablePair(Map<String, String> pair)
	{
		tableNames.add(pair);
	}

	public void addTablePair(String key,String value)
	{
		Map<String, String> pair = new HashMap<String, String>(1,1);
		pair.put(key, value);
		tableNames.add(pair);
	}
	
	public List<ReverseOutput> getOutputSQL()
	{
		return outputSQL;
	}

	public void setOutputSQL(List<ReverseOutput> outputSQL)
	{
		this.outputSQL = outputSQL;
	}

	public TargetDB getTargetDB()
	{
		TargetDB targetDB  = new TargetDB();
		targetDB.setDbIndex(dbIndex);
		for(Map<String, String> map : tableNames)
		{
			if(1 != map.size()){
				throw new IllegalArgumentException("兼容模式不支持多个表或0个表:"+map.size());
			}
			targetDB.addOneTable(map.values().iterator().next());
		}
		targetDB.setOutputSQL(outputSQL);
		return targetDB;
	}

	public Map<String, Field> getRealTableFieldMap() {
		return realTableFieldMap;
	}

	public void setRealTableFieldMap(Map<String, Field> realTableFieldMap) {
		this.realTableFieldMap = realTableFieldMap;
	}
}
