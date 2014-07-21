package com.taobao.tddl.rule.bean;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.rule.LogicTableRule;

public class TDDLRoot {
	final Log log = LogFactory.getLog(TDDLRoot.class);
	protected DBType dbType = DBType.MYSQL;
	protected Map<String/* key */, LogicTable> logicTableMap;
	protected String defaultDBSelectorID;
	//true则使用id in归组优化
	protected boolean needIdInGroup=false;
	//true则使用多表的Distinct支持
	protected boolean completeDistinct=false;
	//ture则使用算库后马上算表的规则计算方式
	protected boolean newTypeRuleCalculate=false;

	/**
	 * 需要注意这个init方法是和内部持有的类的init方法无关的， 虽然可以在放在一个初始化链中
	 */
	public void init() {
		for (Entry<String, LogicTable> logicTableEntry : logicTableMap
				.entrySet()) {
			log.warn("logic Table is starting :" + logicTableEntry.getKey());
			LogicTable logicTable = logicTableEntry.getValue();
			String logicTableName = logicTable.getLogicTableName();
			if (logicTableName == null || logicTableName.length() == 0) {
				// 如果没有指定logicTableName,
				// 那么以map的key作为logicTable的key
				logicTable.setLogicTableName(logicTableEntry.getKey());
			}
			//modify by junyu 2010.10.26 Oracle和Mysql混用改造
			logicTable.setShardRuleDbType(dbType);
			logicTable.init(false);

			log.warn("logic Table inited :" + logicTable.toString());
		}
	}

	public LogicTableRule getLogicTableMap(String logicTableName) {
		LogicTableRule logicTableRule = getLogicTable(logicTableName);
		if (logicTableRule == null) {
			// 逻辑表名不存在于规则表中，尝试从默认表规则寻找，
			// 如果再找不到就抛异常了。
			if (defaultDBSelectorID != null
					&& defaultDBSelectorID.length() != 0) {
				// 如果有默认规则，那么因为默认规则中持有的只有数据源，
				// 需要将表名赋给克隆一份以后的他，这样保证线程安全
				log.debug("use default table rule");
				DefaultLogicTableRule defaultLogicTableRule = new DefaultLogicTableRule(
						defaultDBSelectorID, logicTableName);
				logicTableRule = defaultLogicTableRule;
			} else {
				throw new IllegalArgumentException("未能找到对应规则,逻辑表:"
						+ logicTableName);
			}
		}
		return logicTableRule;
	}

	public LogicTable getLogicTable(String logicTableName) {
		if (logicTableName == null) {
			throw new IllegalArgumentException("logic table name is null");
		}
		
		LogicTable logicTable = logicTableMap.get(logicTableName.toLowerCase());
		return logicTable;
	}

	/**
	 * logicMap的key必须都显示的设置为小写
	 * 
	 * @param logicTableMap
	 */
	public void setLogicTableMap(Map<String, LogicTable> logicTableMap) {
		this.logicTableMap = new HashMap<String, LogicTable>(logicTableMap
				.size());
		for (Entry<String, LogicTable> entry : logicTableMap.entrySet()) {
			String key = entry.getKey();
			if (key != null) {
				key = key.toLowerCase();
			}
			this.logicTableMap.put(key, entry.getValue());
		}
	}

	public Map<String, LogicTable> getLogicTableMap() {
		return Collections.unmodifiableMap(logicTableMap);
	}

	public Object getDBType() {
		return dbType;
	}

	public void setDBType(Object dbType) {
		if (dbType instanceof DBType) {
			this.dbType = (DBType) dbType;
		} else if (dbType instanceof String) {
			this.dbType = DBType.valueOf(((String) dbType).toUpperCase());
		}
	}

	public String getDefaultDBSelectorID() {
		return defaultDBSelectorID;
	}

	public void setDefaultDBSelectorID(String defaultDBSelectorID) {
		this.defaultDBSelectorID = defaultDBSelectorID;
	}

	public boolean isNeedIdInGroup() {
		return needIdInGroup;
	}

	public void setNeedIdInGroup(boolean needIdInGroup) {
		this.needIdInGroup = needIdInGroup;
	}

	public boolean isCompleteDistinct() {
		return completeDistinct;
	}

	public void setCompleteDistinct(boolean completeDistinct) {
		this.completeDistinct = completeDistinct;
	}

	public boolean isNewTypeRuleCalculate() {
		return newTypeRuleCalculate;
	}

	public void setNewTypeRuleCalculate(boolean newTypeRuleCalculate) {
		this.newTypeRuleCalculate = newTypeRuleCalculate;
	}
}
