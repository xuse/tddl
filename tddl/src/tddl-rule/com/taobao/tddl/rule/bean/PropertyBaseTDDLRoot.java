package com.taobao.tddl.rule.bean;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.common.config.beans.TableRule;
import com.taobao.tddl.interact.rule.bean.DBType;

public class PropertyBaseTDDLRoot extends TDDLRoot {
	private Log log = LogFactory.getLog(PropertyBaseTDDLRoot.class);
	public static final String DB_TYPE = "db_type";
	public static final String DEFAULT_DB_INDEX = "default_db_index";
	public static final String TABLE_RULES = "table_rules";
	public static final String DB_INDEXES_SUFFIX = ".db_indexes";
	public static final String TABLE_INDEXES_SUFFIX = ".table_indexes";
	public static final String TABLE_RULEINDEX_SUFFIX = ".table_rule";
	public static final String DB_RULEINDEX_SUFFIX = ".db_rule";
	public static final String TABLE_DB_TYPE=".db_type";
	public static final String DISABLE_FULL_TABLE_SCAN=".disable_full_table_scan";
	public static final String DEFAULT_SEPARATOR = ",";
	public static final String CLONE_TABLE_SEPARATOR = ";";
	
	/**
	 * 
	 * 初始化TableRule,设置必要信息包括
	 *     dbIndexes
	 *     dbRuleArray
	 *     tbRuleArray
	 *     tbSuffix
	 * 
	 * @param prop
	 */
	public void init(Properties prop) {
		String[] tableRules = getLogicTableList(prop);
		setDBType(getDBType(prop));
		setDefaultDBIndex(getDefaultDBIndex(prop));
		Map<String/* key */, LogicTable> logicTableMap = new HashMap<String, LogicTable>(
				tableRules.length);
		for (int i = 0; i < tableRules.length; i++) {
			//这里需要分裂为两个，如果在某一个tableRule中，还有使用;来进行切分的话，那么两个使用相同的规则。只是表名换掉
			String[] cloneSeparator = StringUtil.split(tableRules[i],CLONE_TABLE_SEPARATOR);
			if(cloneSeparator.length == 1){
				TableRule tableRule = new TableRule();
				tableRule.setDbIndexes(getDBIndexes(tableRules[i], prop));
				tableRule.setDbRuleArray(getDBRules(tableRules[i], prop));
				tableRule.setTbRuleArray(getTableRules(tableRules[i], prop));
				tableRule.setTbSuffix(getTabIndexes(tableRules[i], prop));
				tableRule.setDisableFullTableScan(getDisableFullTableScan(tableRules[i],prop));
				DBType dbType=getTableDBType(tableRules[i],prop);
				if(null!=dbType){
				    tableRule.setDbType(dbType);
				}
				tableRule.init();
				logicTableMap.put(tableRules[i].toLowerCase(), tableRule);
			}else{
				for(String str: cloneSeparator){
					str = StringUtil.trim(str);
					if(str.length() == 0){
						continue;
					}
					TableRule tableRule = new TableRule();
					tableRule.setDbIndexes(getDBIndexes(tableRules[i], prop));
					tableRule.setDbRuleArray(getDBRules(tableRules[i], prop));
					tableRule.setTbRuleArray(getTableRules(tableRules[i], prop));
					tableRule.setTbSuffix(getTabIndexes(tableRules[i], prop));
					tableRule.setDisableFullTableScan(getDisableFullTableScan(tableRules[i],prop));
					DBType dbType=getTableDBType(tableRules[i],prop);
					if(null!=dbType){
					    tableRule.setDbType(dbType);
					}
					tableRule.init();
					logicTableMap.put(str.toLowerCase(), tableRule);
				}
			
			}
		}
		
		//setLogicTableMap(logicTableMap); //这里不需要再去搞一遍了
		this.logicTableMap = logicTableMap;
	}
	private void setDefaultDBIndex(String dbIndex){
		if(dbIndex != null && dbIndex.length() != 0){
			this.setDefaultDBSelectorID(dbIndex);
		}
	}
	/**
	 * 得到逻辑表列表
	 * 
	 * @param prop
	 * @return 例如:modDBTab,gmtTab
	 */
	private String[] getLogicTableList(Properties prop) {
		return StringUtil.split(getPropValue(TABLE_RULES, prop),
				DEFAULT_SEPARATOR);
	}

	/**
	 * 得到适用的数据库类型
	 * 
	 * @param prop
	 * @return 例如:mysql
	 */
	private String getDBType(Properties prop) {
		return getPropValue(DB_TYPE, prop);
	}

	/**
	 * 得到适用的数据库类型
	 * 
	 * @param prop
	 * @return 例如:mysql
	 */
	private String getDefaultDBIndex(Properties prop) {
		return getPropValue(DEFAULT_DB_INDEX, prop);
	}
	/**
	 * 得到dbIndexes
	 * 
	 * @param tableName
	 * @param prop
	 * @return 例如: sample_group_0,sample_group_1
	 */
	private String getDBIndexes(String tableName, Properties prop) {
		String key = tableName + DB_INDEXES_SUFFIX;
		return getPropValue(key, prop);
	}

	/**
	 * 得到tableIndexes
	 * 
	 * @param tableName
	 * @param prop
	 * @return 例如：throughAllDB:[_0001-_0004]
	 */
	private String getTabIndexes(String tableName, Properties prop) {
		String key = tableName + TABLE_INDEXES_SUFFIX;
		return getPropValue(key, prop);
	}

	/**
	 * 得到tableRule
	 * 
	 * @param tableName
	 * @param prop
	 * @return 例如:#pk#.longValue() % 4 % 2
	 */
	private Object[] getTableRules(String tableName, Properties prop) {
		String key = tableName + TABLE_RULEINDEX_SUFFIX;
		return getRule(key, prop);
	}

	/**
	 * 得到dbRule
	 * 
	 * @param tableName
	 * @param prop
	 * @return 例如:(#pk#.longValue() % 4).intdiv(2)
	 */
	private Object[] getDBRules(String tableName, Properties prop) {
		String key = tableName + DB_RULEINDEX_SUFFIX;
		return getRule(key, prop);
	}

	/**
	 * 从prop中根据key得到规则， 如果为空，返回null
	 * 
	 * @param key
	 * @param prop
	 * @return
	 */
	private Object[] getRule(String key, Properties prop) {
		Object[] rule = new Object[1];
		rule[0] = getPropValue(key, prop);
		if (rule[0] == null) {
			return null;
		} else {
			return rule;
		}
	}
	
	/**
	 * 从prop中根据key得到全表扫描开关
	 * 
	 * 这里如果disableFullTableScan没有值,那么返回true
	 * @param key
	 * @param prop
	 * @return
	 */
	private boolean getDisableFullTableScan(String tableName,Properties prop){
		String key=tableName+DISABLE_FULL_TABLE_SCAN;
		String disableFullTableScan=getPropValue(key,prop);
		if(null==disableFullTableScan){
			return true;
		}
		return Boolean.valueOf(disableFullTableScan);
	}
	
	/**
	 * 从prop中根据key得到表级别数据库标记
	 * @param tableName
	 * @param prop
	 * @return
	 */
	private DBType getTableDBType(String tableName,Properties prop){
		String key=tableName+TABLE_DB_TYPE;
		String dbType=getPropValue(key,prop);
		if(null==dbType){
			return null;
		}
		return DBType.valueOf(dbType.toUpperCase());
	}

	/**
	 * 从Properties中根据key得到相应值
	 * 
	 * @param key
	 * @param prop
	 * @return
	 */
	private String getPropValue(String key, Properties prop) {
		String value=prop.getProperty(key);
		writeToLog(new String[]{key,"=",value});
		return value;
	}
	
	private StringBuilder sb=new StringBuilder();
	private void writeToLog(String[] pieces){
		sb.delete(0, sb.length());
		for(String p:pieces){
			sb.append(p);
		}
		log.info(sb.toString());
	}
}
