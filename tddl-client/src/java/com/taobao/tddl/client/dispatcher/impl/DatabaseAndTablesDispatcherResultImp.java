package com.taobao.tddl.client.dispatcher.impl;

import java.util.List;

import com.taobao.tddl.client.controller.DatabaseExecutionContext;
import com.taobao.tddl.client.dispatcher.LogicTableName;
import com.taobao.tddl.client.dispatcher.Result;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.sqlobjecttree.DMLCommon;
import com.taobao.tddl.sqlobjecttree.GroupFunctionType;

public class DatabaseAndTablesDispatcherResultImp implements Result {
	final LogicTableName logicTableName;
	final List<TargetDB> target;
	
	public List<TargetDB> getTarget() {
		return target;
	}
	
	public DatabaseAndTablesDispatcherResultImp(List<TargetDB> target
			,LogicTableName logicTableName) {
		this.target =(List<TargetDB>) target;
		this.logicTableName = logicTableName;
	}
	
	public LogicTableName getLogicTableName() {
		return logicTableName;
	}
	
	public GroupFunctionType getGroupFunctionType() {
		return GroupFunctionType.NORMAL;
	}
	
	public LogicTableName getVirtualTableName() {
		return logicTableName;
	}
	
	public int getMax() {
		return DMLCommon.DEFAULT_SKIP_MAX;
	}
	
	public int getSkip() {
		return DMLCommon.DEFAULT_SKIP_MAX;
	}
	
	public List<DatabaseExecutionContext> getDataBaseExecutionContexts()
	{
		return null;
	}
}
