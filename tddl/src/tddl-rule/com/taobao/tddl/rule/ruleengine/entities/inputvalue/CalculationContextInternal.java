package com.taobao.tddl.rule.ruleengine.entities.inputvalue;

import java.util.Map;

import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.RuleChain;
import com.taobao.tddl.rule.ruleengine.rule.ListAbstractResultRule;

public class CalculationContextInternal {
	//public final RuleChain ruleChain;
	//protected Map<String,Set<Object>> result = Collections.emptyMap();
	//public final int index; //参与计算的具体规则是根据index从RuleChain中取得的
	public final ListAbstractResultRule rule;
	public final Map<String/*当前参数要求的列名*/, Comparative> sqlArgs;

	public CalculationContextInternal(RuleChain ruleChain, int index, Map<String/*当前参数要求的列名*/, Comparative> sqlArgs) {
		//this.ruleChain = ruleChain;
		//this.index = index;
		rule = ruleChain.getRuleByIndex(index);
		this.sqlArgs = sqlArgs;
	}

	@Override
	public String toString() {
		return "CalculationContextInternal [rule=" + rule + ", sqlArgs=" + sqlArgs + "]";
	}

}
