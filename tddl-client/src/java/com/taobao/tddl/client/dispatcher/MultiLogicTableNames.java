package com.taobao.tddl.client.dispatcher;

import java.util.Collections;
import java.util.Set;

public class MultiLogicTableNames implements LogicTableName
{
	private Set<String> logicTables = Collections.emptySet();
	
	public String getSingleTable()
	{
		throw new IllegalArgumentException("多表不支持此方法");
	}
	
	public Set<String> getLogicTables()
	{
		return logicTables;
	}
	
	public void setLogicTables(Set<String> logicTables)
	{
		this.logicTables = logicTables;
	}
	
	@Override
	public String toString()
	{
		boolean firstElement = true;
		StringBuilder sb = new StringBuilder();
		for(String tableName : logicTables)
		{
			if(firstElement)
				firstElement = false;
			else
				sb.append(",");
			sb.append(tableName);
		}
		return sb.toString();
	}
}
