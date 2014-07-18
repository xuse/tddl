package com.taobao.tddl.interact.bean;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.interact.sqljep.Comparative;

public class MatcherResultImp implements MatcherResult {
	private final List<TargetDB> calculationResult;
	private final Map<String, Comparative> databaseComparativeMap;
	private final Map<String, Comparative> tableComparativeMap;

	public MatcherResultImp(List<TargetDB> calculationResult, Map<String, Comparative> databaseComparativeMap,
			Map<String, Comparative> tableComparativeMap) {
		this.calculationResult = calculationResult;
		this.databaseComparativeMap = databaseComparativeMap;
		this.tableComparativeMap = tableComparativeMap;
	}

	public List<TargetDB> getCalculationResult() {
		return calculationResult;
	}

	public Map<String, Comparative> getDatabaseComparativeMap() {
		return databaseComparativeMap;
	}

	public Map<String, Comparative> getTableComparativeMap() {
		return tableComparativeMap;
	}
}
