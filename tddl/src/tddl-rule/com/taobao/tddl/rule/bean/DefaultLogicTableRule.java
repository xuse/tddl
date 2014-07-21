package com.taobao.tddl.rule.bean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.interact.bean.Field;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.rule.LogicTableRule;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.RuleChain;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.CalculationContextInternal;

/**
 * 默认的LogicTableRule计算,
 * 
 * @author shenxun
 * 
 */
public class DefaultLogicTableRule implements LogicTableRule, Cloneable {
	private final String defaultTable;
	private final String databases;

	public DefaultLogicTableRule(String databases,String defaultTable) {
		this.databases = databases;
		this.defaultTable = defaultTable;
	}
	
//	/**
//	 * 包内可见，外部不允许设置这个属性
//	 * @param defaultTable
//	 */
//	void setDefaultTableInternal(String defaultTable) {
//		if(this.defaultTable != null){
//			throw new IllegalArgumentException("should not be here ,default table 有值? 内部持有的值"+this.defaultTable
//					+"，输入的值"+defaultTable);
//		}
//		this.defaultTable = defaultTable;
//	}

	public String getDatabases() {
		return databases;
	}

//	public void setDatabases(String databases) {
//		if (databases == null || databases.length() == 0) {
//			throw new IllegalArgumentException("database is null");
//		}
//		if (databases.contains(",")) {
//			throw new IllegalArgumentException("不支持使用多个数据源作为默认数据源");
//		}
//		this.databases = databases;
//	}

	public List<TargetDB> calculate(Map<RuleChain, CalculationContextInternal> map) {
		List<TargetDB> targetDBs = new ArrayList<TargetDB>(0);

		TargetDB targetDB = new TargetDB();
		targetDB.setDbIndex(databases);
		Map<String,Field> tableNames = new HashMap<String,Field>(1,1);
		tableNames.put(defaultTable,Field.EMPTY_FIELD);
		targetDB.setTableNames(tableNames);
		targetDBs.add(targetDB);
		return targetDBs;
	}

	public Set<RuleChain> getRuleChainSet() {
		return Collections.emptySet();
	}

	public List<String> getUniqueColumns() {
		return Collections.emptyList();
	}

	public boolean isAllowReverseOutput() {
		return false;
	}

	public boolean isNeedRowCopy() {
		return false;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		if(databases == null){
			throw new NullPointerException("databases == null || defaultTable == null");
		}
		return super.clone();
	}

	@Override
	public List<TargetDB> calculateNew(RuleContext ruleContext) {
		List<TargetDB> targetDBs = new ArrayList<TargetDB>(0);

		TargetDB targetDB = new TargetDB();
		targetDB.setDbIndex(databases);
		Map<String,Field> tableNames = new HashMap<String,Field>(1,1);
		tableNames.put(defaultTable,Field.EMPTY_FIELD);
		targetDB.setTableNames(tableNames);
		targetDBs.add(targetDB);
		return targetDBs;
	}


}
