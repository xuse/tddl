package com.taobao.tddl.client.jdbc;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;

public class RealSqlContextImp implements RealSqlContext{
	private Map<Integer, ParameterContext> argument;
	private String realTable;
	private String sql;
	
	public Map<Integer, ParameterContext> getArgument() {
		return argument;
	}
	
	public void setArgument(Map<Integer, ParameterContext> argument) {
		this.argument = argument;
	}
	
	public String getRealTable() {
		return realTable;
	}
	
	public void setRealTable(String realTable) {
		this.realTable = realTable;
	}
	
	public String getSql() {
		return sql;
	}
	
	public void setSql(String sql) {
		this.sql = sql;
	}
	
	@Override
	public String toString() {
		return "RealSqlContextImp [argument=" + argument + ", realTable="
				+ realTable + ", sql=" + sql + "]";
	}
}
