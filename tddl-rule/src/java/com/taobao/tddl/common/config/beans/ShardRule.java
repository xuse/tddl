package com.taobao.tddl.common.config.beans;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.rule.bean.LogicTable;
import com.taobao.tddl.rule.bean.TDDLRoot;

/**
 * 一份完整的分库分表规则配置，一套库一份
 *  
 * @author linxuan
 */
public class ShardRule extends TDDLRoot implements Cloneable {
	public static final String DBINDEX_SUFFIX_READ = "_r";
	public static final String DBINDEX_SUFFIX_WRITE = "_w";
	
	/**
	 * spring的bug：当getter返回值和setter参数类型不同时会报错
	 */
	public DBType getDbType() {
		return dbType;
	}

	public String getDbtype() {
		return dbType.toString();
	}

	public void setDbtype(String dbtype) {
		super.dbType = DBType.valueOf(dbtype);
	}

	public Map<String, LogicTable> getTableRules() {
		return logicTableMap;
	}

	public void setTableRules(Map<String, LogicTable> tableRules) {
		Map<String, LogicTable> lowerKeysLogicTableMap = new HashMap<String, LogicTable>(tableRules.size());
		for(Entry<String, LogicTable> entry: tableRules.entrySet()){
			lowerKeysLogicTableMap.put(entry.getKey().toLowerCase(),entry.getValue());
		}
		this.logicTableMap = lowerKeysLogicTableMap;
	}
	
	public String getDefaultDbIndex() {
		return defaultDBSelectorID;
	}

	public void setDefaultDbIndex(String defaultDbIndex) {
		this.defaultDBSelectorID = defaultDbIndex;
	}
}
