package com.taobao.tddl.rule.ruleengine.impl.type;

import java.util.Collection;
import java.util.List;

import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;

public interface TableNameTypeHandler {
	List<String> buildPhysicTab(List<Object> xxxfixes, TabRule tab,
			String vTab);

	String buildOnePhsicTab(Object xxxfixInt, TabRule tab, String vtab);

	Collection<String> buildAllPassableTable(
			Collection<String> collection, int step, String vTab,
			int rStart, int rEnd, String padding, int placeholderbit);
	
	Collection<String> buildAllPassableTable(
			Collection<String> collection, int step, int step2, String vTab,
			int rStart, int sStart, int rEnd, int sEnd, String padding, int placeholderbit);

}
