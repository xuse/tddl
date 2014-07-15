package com.taobao.tddl.client.dispatcher;

import com.alibaba.common.lang.StringUtil;

public class SingleLogicTableName implements LogicTableName
{
	private String tableName;

	public String getSingleTable()
	{
		return StringUtil.toLowerCase(tableName);
	}
	
	public SingleLogicTableName(String tableName)
	{
		super();
		this.tableName = tableName;
	}

	@Override
	public String toString()
	{
		return tableName;
	}
}
