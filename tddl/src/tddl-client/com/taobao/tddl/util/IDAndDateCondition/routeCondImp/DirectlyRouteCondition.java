package com.taobao.tddl.util.IDAndDateCondition.routeCondImp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.controller.OrderByMessages;
import com.taobao.tddl.client.controller.OrderByMessagesImp;
import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.sqlobjecttree.DMLCommon;
import com.taobao.tddl.sqlobjecttree.GroupFunctionType;


public class DirectlyRouteCondition implements RouteCondition{
	/**
	 * 默認是mysql
	 */
	protected DBType dbType = DBType.MYSQL;
	protected int skip = DMLCommon.DEFAULT_SKIP_MAX;
	protected String suffix ;
	boolean isSuffixModel = false;

	protected int max = DMLCommon.DEFAULT_SKIP_MAX;
	/**
	 * 默认为空
	 */
	@SuppressWarnings("unchecked")
	protected OrderByMessages orderByMessages = new OrderByMessagesImp(Collections.EMPTY_LIST);
	/**
	 * 默认为空
	 */
	protected GroupFunctionType groupFunctionType = GroupFunctionType.NORMAL;
	protected ROUTE_TYPE routeType = ROUTE_TYPE.FLUSH_ON_EXECUTE;
	/**
	 * 目标表的id
	 */
	protected Set<String> tables = new HashSet<String>(2);
	protected String virtualTableName;
	/**
	 * 目标库的id
	 */
	protected String dbRuleID;
	public Set<String> getTables() {
			return tables;
	}

	public void setDBType(DBType dbType) {
		this.dbType = dbType;
	}
	
	public DBType getDBType() {
		return dbType;
	}

	public void setTables(Set<String> tables) {
		this.tables = tables;
	}
	
	public void addATable(String table){
		tables.add(table);
	}
	public String getVirtualTableName() {
		return virtualTableName;
	}
	/**
	 * 虚拟表名
	 * @param virtualTableName
	 */
	public void setVirtualTableName(String virtualTableName){
		this.virtualTableName=virtualTableName;
	}
	public String getDbRuleID() {
		return dbRuleID;
	}
//	/**
//	 * 规则的id，和db id同级。
//	 * 
//	 * 在进行判断的时候，会先查看规则文件内是否有对应的规则。
//	 * 
//	 * 如果没有，则会搜索数据库map内是否有对应的，再没有则报错。
//	 * 
//	 * @param dbRuleID
//	 */
//	public void setDbRuleID(String dbRuleID) {
//		this.dbRuleID = dbRuleID;
//	}

	/**
	 * 规则的id，和db id同级。
	 * 
	 * 在进行判断的时候，会先查看规则文件内是否有对应的规则。
	 * 
	 * 如果没有，则会搜索数据库map内是否有对应的，再没有则报错。
	 * 
	 * @param dbId
	 */
	public void setDBId(String dbId){
		this.dbRuleID = dbId;
	}
	
	public ROUTE_TYPE getRouteType() {
		return routeType;
	}
	
	/**
	 * 获取skip值
	 * 因为TDDL不是数据库，所以这里做了一个假定：
	 * 在所有含义为skip的数据中，最大的那个永远是有意义的。
	 * 
	 * 多层嵌套中也是如此。
	 * @return
	 */
	public int getSkip(){
		return skip;
	}

	/**
	 * 获取max值。
	 * 因为TDDL不是数据库，所以这里做了一个假定：
	 * 在所有含义为max的数据中，最大的那个永远是有意义的。
	 * 
	 * 多层嵌套中也是如此。
	 * @return
	 */
	public int getMax(){
		return max;
	}

	/**
	 * 获取order by 信息
	 * @return
	 */
	public OrderByMessages getOrderByMessages(){
		return orderByMessages;
	}

	/**
	 * 获取当前sql的select | columns | from
	 * 中columns的类型
	 * 如果为max min count等，那么类型会有相应变化
	 * 同时如果group function和其他列名字段混用，则这里会返回NORMAL
	 * @return
	 */
	public GroupFunctionType getGroupFunctionType(){
		return groupFunctionType;
	}
	public String getSuffix() {
		return suffix;
	}
	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}
	
	public Map<String/*db index key */, List<Map<String/* original table */, String/* targetTable */>>> getShardTableMap() {
	
		List<Map<String/* original table */, String/* targetTable */>> tableList = new ArrayList<Map<String, String>>(
				1);
		for (String targetTable : tables) {
			Map<String/* original table */, String/* target table */> table = new HashMap<String, String>(
					tables.size());
			table.put(virtualTableName, targetTable);
			if(!table.isEmpty()){
				tableList.add(table);
			}
		}

		Map<String/* key */, List<Map<String/* original table */, String/* targetTable */>>> shardTableMap = new HashMap<String, List<Map<String, String>>>(
				2);
		shardTableMap.put(dbRuleID, tableList);
		return shardTableMap;
	}
	public boolean isSuffixModel() {
		return isSuffixModel;
	}
	public void setSuffixModel(boolean isSuffixModel) {
		this.isSuffixModel = isSuffixModel;
	}

	public DBType getDbType() {
		return dbType;
	}

	public void setDbType(DBType dbType) {
		this.dbType = dbType;
	}

	public void setSkip(int skip) {
		this.skip = skip;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public void setOrderByMessages(OrderByMessages orderByMessages) {
		this.orderByMessages = orderByMessages;
	}

	public void setGroupFunctionType(GroupFunctionType groupFunctionType) {
		this.groupFunctionType = groupFunctionType;
	}

	public void setRouteType(ROUTE_TYPE routeType) {
		this.routeType = routeType;
	}
	
}
