package com.taobao.tddl.util.IDAndDateCondition.routeCondImp;


import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.sqlobjecttree.ColumnValuePairAndMatchRulesIndex;
import com.taobao.tddl.sqlobjecttree.DMLCommon;
import com.taobao.tddl.sqlobjecttree.GroupFunctionType;
import com.taobao.tddl.sqlobjecttree.InExpressionObject;
import com.taobao.tddl.sqlobjecttree.OrderByEle;
import com.taobao.tddl.sqlobjecttree.SqlAndTableAtParser;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;
import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.HandlerContainer;

public class DummySqlParcerResult implements SqlParserResult {

    final ComparativeMapChoicer choicer;
    final Set<String> logicTableName;
    List<OrderByEle> orderBys = Collections.emptyList();
    List<OrderByEle> groupBys = Collections.emptyList();
    int max = DMLCommon.DEFAULT_SKIP_MAX;
    int skip = DMLCommon.DEFAULT_SKIP_MAX;
    GroupFunctionType groupFunctionType = GroupFunctionType.NORMAL;
    List<String> distinctColumn=Collections.emptyList();
    boolean hasHavingCondition=false;
    List<InExpressionObject> inObjList=Collections.emptyList();
    
    public DummySqlParcerResult(ComparativeMapChoicer choicer, String logicTableName) {
        this.choicer = choicer;
        Set<String> logicTableNames = new HashSet<String>();
        logicTableNames.add(logicTableName);
        this.logicTableName = logicTableNames;
    }
    
    public DummySqlParcerResult(SimpleCondition simpleCondition) {
    	Set<String> tempSet = new HashSet<String>();
    	if(simpleCondition.getVirtualTableName()!=null){
    	    tempSet.add(simpleCondition.getVirtualTableName());
    	}else if(simpleCondition.getVirtualTableForJoin()!=null){
    		tempSet=simpleCondition.getVirtualTableForJoin();
    	}
    	this.logicTableName =tempSet;
    	this.choicer = simpleCondition;
    	this.orderBys = simpleCondition.getOrderBys();
    	this.groupBys = simpleCondition.getGroupBys();
    	this.max = simpleCondition.getMax();
    	this.skip = simpleCondition.getSkip();
    	this.groupFunctionType = simpleCondition.getGroupFunctionType();
    	this.distinctColumn=simpleCondition.getDistinctColumn();
    	this.hasHavingCondition=simpleCondition.hasHavingCondition();
    	this.inObjList=simpleCondition.getInObjList();
    }

    public ColumnValuePairAndMatchRulesIndex getColumnsMapLists(
            List<Object> arguments, List<Set<String>> uniqueKeySetList,
            List<Set<String>> databaseSharedingSetList,
            List<Set<String>> tableShardingSetList) {
        return null;
    }

    public ComparativeMapChoicer getComparativeMapChoicer() {
        return choicer;
    }

    public GroupFunctionType getGroupFuncType() {
        return groupFunctionType;
    }

    public int getMax(List<Object> param) {
        return max;
    }

    public List<OrderByEle> getOrderByEles() {
        return orderBys;
    }

    public int getSkip(List<Object> param) {
        return skip;
    }

    public Set<String> getTableName() {
        return logicTableName;
    }

    public List<OrderByEle> getGroupByEles() {
        return groupBys;
    }

	public List<SqlAndTableAtParser> getSqlReadyToRun(
			Collection<Map<String, String>> tables, List<Object> args,
			HandlerContainer handlerContainer)
	{
		 return Collections.emptyList();
	}

	@Override
	public List<String> getDistinctColumn() {
		return this.distinctColumn;
	}
	
	public boolean hasHavingCondition(){
		return this.hasHavingCondition;
	}

	@Override
	public List<InExpressionObject> getInExpressionObjectList() {
		return this.inObjList;
	}
	
	@Override
	public String getFormatSql() {
		return null;
	}

}
