package com.taobao.tddl.rule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.rule.bean.RuleContext;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.RuleChain;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.CalculationContextInternal;

public interface LogicTableRule {
	Set<RuleChain> getRuleChainSet();

	boolean isNeedRowCopy();

//	void setNeedRowCopy(boolean needRowCopy);

	boolean isAllowReverseOutput();

//	void setAllowReverseOutput(boolean allowReverseOutput);
	
	/**
	 * 不同的节点领走自己的结果，并根据结果进行1对多映射
	 * @param map
	 * @return
	 */
	public List<TargetDB> calculate(
			Map<RuleChain, CalculationContextInternal> map);
	
	public List<TargetDB> calculateNew(
			RuleContext ruleContext);
	
	public List<String> getUniqueColumns();
	
//	public Map<String, ? extends SharedElement> getSubSharedElements();
//	DBType getDBType();
}
