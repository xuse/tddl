package com.taobao.tddl.rule.ruleengine.entities.abstractentities;

import java.util.List;

import com.taobao.tddl.rule.ruleengine.rule.ListAbstractResultRule;

public interface TableListResultRuleContainer {
	/**
	 * 将全局表规则设置给表规则容器。
	 * 如果设置成功则返回true;
	 * 如果设置失败则返回false;
	 * 
	 * @param listResultRule
	 * @return
	 */
	public boolean setTableListResultRule(List<ListAbstractResultRule> listResultRule);
}
