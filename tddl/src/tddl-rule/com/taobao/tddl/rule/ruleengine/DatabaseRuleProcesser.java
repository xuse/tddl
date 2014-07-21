package com.taobao.tddl.rule.ruleengine;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.LogicTabMatrix;

public interface DatabaseRuleProcesser {
	/**
	 * 根据虚拟表名，sql中分库分表信息字段，以及配置文件，获取分库的源信息
	 * @param virtualTabName
	 * @param colMap
	 * @param logTabs
	 * @return
	 */
	public List<TargetDB> process(String virtualTabName,
			Map<String, Comparative> colMap,LogicTabMatrix logTabs);
}
