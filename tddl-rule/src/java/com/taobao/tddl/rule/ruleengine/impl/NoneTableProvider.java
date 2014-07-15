package com.taobao.tddl.rule.ruleengine.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.exception.checked.TDLCheckedExcption;
import com.taobao.tddl.common.sequence.Config;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.ruleengine.TableRuleProvider;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;

public class NoneTableProvider implements TableRuleProvider {

	public Set<String> getTables(Comparable<?>[] row,
			Map<String, Integer> position, TabRule tab, String tabName)
			throws TDLCheckedExcption {
		Set<String> li = new HashSet<String>();

		String obj = tabName;
		li.add(obj);
		return li;
	}

	public Set<String> getTables(Map<String, Comparative> map,
			TabRule tab, String tabName, Config config) throws TDLCheckedExcption {
		Set<String> li = new HashSet<String>();

		String obj = tabName;
		li.add(obj);
		return li;
	}

}
