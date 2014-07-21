package com.taobao.tddl.rule.ruleengine.entities.inputvalue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.interact.rule.bean.DBType;

/**
 * 对应一张虚拟表的多个子DBRule
 * 
 * @author shenxun
 * 
 */
@SuppressWarnings("unchecked")
public class LogicTabMatrix {
	/**
	 * 允许反向输出
	 */
	private boolean allowReverseOutput;

	/*public enum DB_TYPE {
		ORACLE, MYSQL
	}*/

	/**
	 * 默认的解析规则
	 */
	private DBType dbType = DBType.MYSQL;
	/**
	 * 虚拟表名，只用在表名替换的时候，因此这个tableName还保持原来的大小写
	 */
	private String tableName;

	/**
	 * 供选虚拟库规则Map
	 */
	private Map<String, DBRule> depositedRules = Collections.EMPTY_MAP;

	/**
	 * 所有规则存放的Map,包含那些没有expression String字段，只用于defaultRule的keyvalue对
	 */
	private Map<String, DBRule> allRules = Collections.EMPTY_MAP;

	/**
	 * 默认虚拟库规则列表
	 */
	private List<DBRule> defaultRules = new ArrayList<DBRule>();

	private TabRule globalTableRule = null;

	/**
	 * 是否需要反向输出
	 */
	private boolean needRowCopy = false;

	/**
	 * 表生成因子 用于在虚拟表名和表后缀之间的分割
	 */
	private String tableFactor = null;

	public Map<String, DBRule> getAllRules() {
		return allRules;
	}

	public void setAllRules(Map<String, DBRule> allRules) {
		this.allRules = allRules;
	}

	public TabRule getGlobalTableRule() {
		return globalTableRule;
	}

	public void setGlobalTableRule(TabRule globalTableRule) {
		this.globalTableRule = globalTableRule;
	}

	/**
	 * 只用在表名替换的时候，因此这个tableName还保持原来的大小写
	 * 
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		if (tableName != null) {
			this.tableName = tableName.toLowerCase();
		} else {
			this.tableName = "";
		}
	}

	public Map<String, DBRule> getDepositedRules() {
		return depositedRules;
	}

	public void setDepositedRules(Map<String, DBRule> depositedRules) {
		this.depositedRules = depositedRules;
	}

	public List<DBRule> getDefaultRules() {
		return defaultRules;
	}

	public void setDefaultRules(List<DBRule> defaultRules) {
		this.defaultRules = defaultRules;
	}

	public boolean isNeedRowCopy() {
		return needRowCopy;
	}

	public void setNeedRowCopy(boolean needRowCopy) {
		this.needRowCopy = needRowCopy;
	}

	public String getTableFactor() {
		return tableFactor;
	}

	public void setTableFactor(String tableFactor) {
		if (tableFactor != null) {
			this.tableFactor = tableFactor.toLowerCase();
		}

	}

	public DBType getDBType() {
		return dbType;
	}

	public void setDBType(String dbType) {
		if (dbType != null && !dbType.equals("")) {
			this.dbType = DBType.valueOf(dbType.toUpperCase());
		}
	}

	public boolean isAllowReverseOutput() {
		return allowReverseOutput;
	}

	public void setAllowReverseOutput(boolean allowReverseOutput) {
		this.allowReverseOutput = allowReverseOutput;
	}

}
