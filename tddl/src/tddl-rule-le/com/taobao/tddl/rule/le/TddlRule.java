//Copyright(c) Taobao.com
package com.taobao.tddl.rule.le;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.bean.Field;
import com.taobao.tddl.interact.bean.MatcherResult;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.interact.rule.Rule;
import com.taobao.tddl.interact.rule.Rule.RuleColumn;
import com.taobao.tddl.interact.rule.VirtualTable;
import com.taobao.tddl.interact.rule.VirtualTableRoot;
import com.taobao.tddl.interact.rule.VirtualTableRuleMatcher;
import com.taobao.tddl.interact.rule.bean.SqlType;
import com.taobao.tddl.rule.le.bean.TargetDatabase;
import com.taobao.tddl.rule.le.exception.ResultCompareDiffException;
import com.taobao.tddl.rule.le.extend.MatchResultCompare;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-3-29上午11:02:23
 */
public class TddlRule extends TddlRuleInner implements TddlRuleExtend,
		TddlRuleMetaData {
	private final VirtualTableRuleMatcher matcher = new VirtualTableRuleMatcher();

	public Map<String, Set<String>> getTopologyByVersion(String vtab,
			String version) {
		VirtualTableRoot root = super.vtrs.get(version);
		if (root == null) {
			return null;
		}
		VirtualTable vt = root.getTableRules().get(vtab);

		// add by jiechen.qzm
		if (vt == null) {
			return defaultTopology(vtab, root);
		}
		return vt.getActualTopology();
	}

	@Override
	public Set<String> getTableShardColumn(String logicTable) {
		Set<String> shardColumn = this.shardColumnCache.get(logicTable);
		if (null != shardColumn && !shardColumn.isEmpty()) {
			return shardColumn;
		}
		// 如果cache没命中，则从规则里找分库分表字段
		shardColumn = new HashSet<String>();
		VirtualTableRoot virtualTableRoot = super.vtrs.get(this.versionIndex
				.get(0));
		if (null != virtualTableRoot
				&& virtualTableRoot.getVirtualTableMap()
						.containsKey(logicTable)) {
			VirtualTable virtualTable = virtualTableRoot.getVirtualTableMap()
					.get(logicTable);
			if (null != virtualTable) {
				List<Rule<String>> dbRules = virtualTable.getDbShardRules();
				List<Rule<String>> tableRules = virtualTable.getTbShardRules();
				if (null != dbRules) {
					for (Rule<String> rule : dbRules) {
						Map<String, RuleColumn> columRule = rule
								.getRuleColumns();
						if (null != columRule && !columRule.isEmpty()) {
							shardColumn.addAll(columRule.keySet());
						}
					}
				}
				if (null != tableRules) {
					for (Rule<String> rule : tableRules) {
						Map<String, RuleColumn> columRule = rule
								.getRuleColumns();
						if (null != columRule && !columRule.isEmpty()) {
							shardColumn.addAll(columRule.keySet());
						}
					}
				}
			}
		}
		if (!shardColumn.isEmpty()) {
			// 如果走规则找到了将结果放入缓存
			this.shardColumnCache.put(logicTable, shardColumn);
		}
		return shardColumn;
	}

	public Map<String/* version */, VirtualTableRoot> getAllVersionedRule() {
		return super.vtrs;
	}

	public VirtualTableRoot getLocalRule() {
		if (super.vtr == null) {
			return null;
		}
		return super.vtr;
	}

	public Map<String, Set<String>> getLocalRuleTopology(String vtab) {
		if (super.vtr == null) {
			return null;
		}
		VirtualTable vt = super.vtr.getTableRules().get(vtab);

		// add by jiechen.qzm
		if (vt == null) {
			return defaultTopology(vtab, vtr);
		}
		return vt.getActualTopology();
	}

	public List<TargetDB> routeWithTargetDbResult(String vtab,
			ComparativeMapChoicer choicer) {
		return this.route0(vtab, choicer, getVirtualTableRoot());
	}

	public Map<String, List<TargetDatabase>> routeMultiVersion(String vtab,
			ComparativeMapChoicer choicer) {
		if (this.vtr != null && this.vtrs.size() == 0) {
			throw new RuntimeException(
					"routeWithMulVersion method just support multy version rule,use route method instead or config with multy version style!");
		}

		// modified by jiechen.qzm
		Map<String, List<TargetDatabase>> results = new HashMap<String, List<TargetDatabase>>();
		for (Map.Entry<String, VirtualTableRoot> entry : this.vtrs.entrySet()) {
			VirtualTableRoot virtualTableRoot = entry.getValue();
			VirtualTable rule = virtualTableRoot.getVirtualTable(vtab);
			List<TargetDB> dbs;
			if (rule == null) {
				dbs = defaultRoute(vtab, virtualTableRoot);
			} else {
				MatcherResult result = matcher
						.match(true, choicer, null, rule);
				if (result != null) {
					dbs = result.getCalculationResult();
				} else {
					continue;
				}
			}
			List<TargetDatabase> re = convert2TargetDatabase(dbs);
			results.put(entry.getKey(), re);
		}
		return results;
	}

	public List<TargetDatabase> route(String vtab, String conditionStr) {
		return this.route(vtab, conditionStr, getVirtualTableRoot());
	}

	public List<TargetDB> routeWithTargetDbResult(String vtab,
			String conditionStr) {
		return this.route0(vtab, conditionStr, this.vtr);
	}

	// modified by jiechen.qzm
	// method reuse
	public Map<String, List<TargetDatabase>> routeMultiVersion(String vtab,
			String conditionStr) {
		ComparativeMapChoicer choicer = this
				.generateComparativeMapChoicer(conditionStr);
		return routeMultiVersion(vtab, choicer);
	}

	/*
	 * public Map<String, List<TargetDatabase>> routeMultiVersion(String vtab,
	 * String conditionStr) { if (this.vtr != null && this.vtrs.size() == 0) {
	 * throw new RuntimeException(
	 * "routeWithMulVersion method just support multy version rule,use route method instead or config with multy version style!"
	 * ); }
	 * 
	 * Map<String, List<TargetDatabase>> results = new HashMap<String,
	 * List<TargetDatabase>>(); for (Map.Entry<String, VirtualTableRoot> entry :
	 * this.vtrs.entrySet()) { VirtualTable rule =
	 * entry.getValue().getVirtualTable(vtab); ComparativeMapChoicer choicer =
	 * this .generateComparativeMapChoicer(conditionStr); MatcherResult result =
	 * matcher.match(false, choicer, null, rule); if (result != null) {
	 * List<TargetDB> dbs = result.getCalculationResult(); List<TargetDatabase>
	 * re = comvert2TargetDatabase(dbs); results.put(entry.getKey(), re); } }
	 * return results; }
	 */

	public List<TargetDatabase> routeWithSpecifyRuleVersion(String vtab,
			String conditionStr, String version) {
		if (this.vtr != null && this.vtrs.size() == 0) {
			throw new RuntimeException(
					"routeWithMulVersion method just support multy version rule,use route method instead or config with multy version style!");
		}

		return this.route(vtab, conditionStr, this.vtrs.get(version));
	}

	public List<TargetDB> routeMultiVersionAndCompareT(SqlType sqlType,
			String vtab, String conditionStr) throws ResultCompareDiffException {
		return routeMultiVersionAndCompareT(sqlType, vtab, conditionStr, null,
				null);
	}

	public List<TargetDB> routeMultiVersionAndCompareT(SqlType sqlType,
			String vtab, String conditionStr, String oriDb, String oriTable)
			throws ResultCompareDiffException {
		if (this.vtr != null && this.vtrs.size() == 0) {
			throw new RuntimeException(
					"routeWithMulVersion method just support multy version rule,use route method instead or config with multy version style!");
		}

		/*
		 * 这个逻辑做了修改。将使用规则版本的逻辑，全部交给 versionIndex 来决定
		 * 
		 * 
		// 如果只有单套规则,直接返回这套规则的路由结果
		if (this.vtrs.size() == 1) {
			return route0(vtab, conditionStr,
					this.vtrs.get(versionIndex.get(0)));
		}

		// 如果不止一套规则,那么计算两套规则,默认都返回旧规则计算结果
		if (this.vtrs.size() != 2 || this.versionIndex.size() != 2) {
			throw new RuntimeException(
					"not support more than 2 copy rule compare");
		}*/
		
		if (this.versionIndex.size() == 1) {
			return route0(vtab, conditionStr,
					this.vtrs.get(versionIndex.get(0)));
		}
		
		if (this.versionIndex.size() != 2) {
			throw new RuntimeException(
					"not support more than 2 copy rule compare");
		}

		// 第一个排位的为旧规则
		VirtualTableRoot oldVirtualTableRoot = vtrs.get(versionIndex.get(0));
		VirtualTable oldRule = oldVirtualTableRoot.getVirtualTable(vtab);
		List<TargetDB> oldTarget = null;
		if (oldRule == null) {
			oldTarget = defaultRoute(vtab, oldVirtualTableRoot);
		} else {
			oldTarget = this.getTargetDb(oldRule, conditionStr);
		}

		// 如果为select,直接返回旧规则计算结果
		if (sqlType.equals(SqlType.SELECT)
				|| sqlType.equals(SqlType.SELECT_FOR_UPDATE)) {
			return oldTarget;
		}

		// 第二个排位的为新规则
		VirtualTableRoot newVirtualTableRoot = vtrs.get(versionIndex.get(1));
		VirtualTable newRule = newVirtualTableRoot.getVirtualTable(vtab);
		List<TargetDB> newTarget = null;
		if (oldRule == null) {
			newTarget = defaultRoute(vtab, oldVirtualTableRoot);
		} else {
			newTarget = this.getTargetDb(newRule, conditionStr);
		}

		// 进行比较
		boolean compareResult = MatchResultCompare.targetDbCompare(newTarget,
				oldTarget, oriDb, oriTable);

		if (compareResult) {
			return oldTarget;
		} else {
			throw new ResultCompareDiffException(
					"sql type is not-select,rule calculate result diff");
		}
	}

	public List<TargetDatabase> convert2TargetDatabase(List<TargetDB> dbs) {
		List<TargetDatabase> re = new ArrayList<TargetDatabase>();
		for (TargetDB db : dbs) {
			TargetDatabase td = new TargetDatabase();
			td.setDbIndex(db.getDbIndex());
			td.setTableNames(new ArrayList<String>(db.getTableNames()));
			td.setTableNamesWithSourceKeys(db.getTableNameMap());
			re.add(td);
		}
		return re;
	}

	private List<TargetDB> getTargetDb(VirtualTable rule, String conditionStr) {
		ComparativeMapChoicer choicer = this
				.generateComparativeMapChoicer(conditionStr);
		MatcherResult result = matcher.match(true, choicer, null, rule);
		if (result != null) {
			return result.getCalculationResult();
		} else {
			return null;
		}
	}

	private List<TargetDatabase> route(String vtab, String conditionStr,
			VirtualTableRoot vtrCurrent) {
		List<TargetDB> dbs = this.route0(vtab, conditionStr, vtrCurrent);
		if (dbs != null) {
			List<TargetDatabase> re = new ArrayList<TargetDatabase>();
			for (TargetDB db : dbs) {
				TargetDatabase td = new TargetDatabase();
				td.setDbIndex(db.getDbIndex());
				td.setTableNames(new ArrayList<String>(db.getTableNames()));
				td.setTableNamesWithSourceKeys(db.getTableNameMap());
				re.add(td);
			}
			return re;
		} else {
			return null;
		}
	}

	private List<TargetDB> route0(String vtab, String conditionStr,
			VirtualTableRoot vtrCurrent) {
		ComparativeMapChoicer choicer = this
				.generateComparativeMapChoicer(conditionStr);
		return route0(vtab, choicer, vtrCurrent);
	}

	private List<TargetDB> route0(String vtab, ComparativeMapChoicer choicer,
			VirtualTableRoot vtrCurrent) {
		VirtualTable rule = vtrCurrent.getVirtualTable(vtab);
		if (rule == null) {
			return defaultRoute(vtab, vtrCurrent);
		}
		MatcherResult result = matcher.match(true, choicer, null, rule);
		if (result != null) {
			return result.getCalculationResult();
		} else {
			return null;
		}
	}

	/**
	 * 没有分库分表的逻辑表，返回指定库表
	 * 
	 * @param vtab
	 * @param vtrCurrent
	 * @return
	 */
	private List<TargetDB> defaultRoute(String vtab, VirtualTableRoot vtrCurrent) {
		List<TargetDB> result = new ArrayList<TargetDB>(1);
		TargetDB targetDb = new TargetDB();
		targetDb.setDbIndex(this.getDefaultDbIndex(vtab, vtrCurrent));
		Map<String, Field> tableNames = new HashMap<String, Field>(1);
		tableNames.put(vtab, null);
		targetDb.setTableNames(tableNames);
		result.add(targetDb);
		return result;
	}

	/**
	 * 没有分库分表的逻辑表，返回指定库表
	 * 
	 * @param vtab
	 * @param vtrCurrent
	 * @return
	 */
	private Map<String, Set<String>> defaultTopology(String vtab,
			VirtualTableRoot vtrCurrent) {
		Map<String, Set<String>> result = new HashMap<String, Set<String>>(1);
		Set<String> tables = new HashSet<String>();
		tables.add(vtab);
		result.put(this.getDefaultDbIndex(vtab, vtrCurrent), tables);
		return result;
	}

	/**
	 * 没有分库分表的逻辑表，先从dbIndex中获取映射的库，没有则返回默认的库
	 * 
	 * @param vtab
	 * @param vtrCurrent
	 * @return
	 */
	private String getDefaultDbIndex(String vtab, VirtualTableRoot vtrCurrent) {
		Map<String, String> dbIndexMap = vtrCurrent.getDbIndexMap();
		if (dbIndexMap != null && dbIndexMap.get(vtab) != null) {
			return dbIndexMap.get(vtab);
		}
		return vtrCurrent.getDefaultDbIndex();
	}
}
