package com.taobao.tddl.rule.ruleengine.entities.convientobjectmaker;

import java.util.Map;

import com.taobao.tddl.rule.bean.Database;

public interface DatabaseMapProvider {
	public Map<String, Database> getDatabaseMap();
}
