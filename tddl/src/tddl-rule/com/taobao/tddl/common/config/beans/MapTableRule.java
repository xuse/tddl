package com.taobao.tddl.common.config.beans;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.taobao.tddl.rule.bean.Database;
import com.taobao.tddl.rule.bean.LogicTable;
import com.taobao.tddl.rule.bean.Table;
import com.taobao.tddl.rule.groovy.GroovyListRuleEngine;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.RuleChain;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.SharedElement;
import com.taobao.tddl.rule.ruleengine.entities.convientobjectmaker.DatabaseMapProvider;
import com.taobao.tddl.rule.ruleengine.entities.convientobjectmaker.TableMapProvider;
import com.taobao.tddl.rule.ruleengine.util.RuleUtils;

/**
 * 允许自己指定所有分库和分表。 但规则必须统一。也就是表规则链和库规则链全局唯一
 * 
 * @author shenxun
 */
public class MapTableRule extends LogicTable {
	public MapTableRule() {
	}

	/**
	 * 库的map.key就是计算规则后获取的key,value就是
	 */
	private Map<String, String> dbMap = null;
	private Map<String, String> tabMap = null;

	List<Object> dbShardingRule = new LinkedList<Object>();
	List<Object> tabShardingRule = new LinkedList<Object>();
    
	@Override
	public void init() {
		init(true);
	}

	@Override
	public void init(boolean invokeBySpring) {
		final Map<String, String> tempDBMap = dbMap;
		DatabaseMapProvider mapProvider = new DatabaseMapProvider() {
			public Map<String, Database> getDatabaseMap() {
				Map<String, Database> retMap = new HashMap<String, Database>(
						tempDBMap.size());
				for (Entry<String, String> entry : tempDBMap.entrySet()) {
					Database db = new Database();
					db.setDataSourceKey(entry.getValue());
					retMap.put(entry.getKey(), db);
				}
				return retMap;
			}
		};
		// 设置dbMap
		setDatabaseMapProvider(mapProvider);

		// 设置tabMap
		final Map<String, String> tempTabMap = tabMap;
		TableMapProvider tabMap = new TableMapProvider() {

			public void setParentID(String parentID) {
				// needn't do anything.
			}

			public void setLogicTable(String logicTable) {
				// needn't do anything
			}

			public Map<String, SharedElement> getTablesMap() {
				Map<String, SharedElement> retMap = new HashMap<String, SharedElement>(
						tempTabMap.size());
				for (Entry<String, String> entry : tempTabMap.entrySet()) {
					Table table = new Table();
					table.setTableName(entry.getValue());
					retMap.put(entry.getKey(), table);
				}
				return retMap;
			}
		};
		setTableMapProvider(tabMap);

		// 初始化规则
		boolean isDatabase = true;
		RuleChain rc = RuleUtils.getRuleChainByRuleStringList(dbShardingRule,
				GroovyListRuleEngine.class, isDatabase,extraPackagesStr);
		super.listResultRule = rc;

		rc = RuleUtils.getRuleChainByRuleStringList(tabShardingRule,
				GroovyListRuleEngine.class, !isDatabase,extraPackagesStr);
		setTableRuleChain(rc);
		super.init(invokeBySpring);
	}

	public List<Object> getDbShardingRule() {
		return dbShardingRule;
	}

	public void setDbShardingRule(List<Object> dbShardingRule) {
		this.dbShardingRule = dbShardingRule;
	}

	public List<Object> getTabShardingRule() {
		return tabShardingRule;
	}

	public void setTabShardingRule(List<Object> tabShardingRule) {
		this.tabShardingRule = tabShardingRule;
	}
}
