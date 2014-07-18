//Copyright(c) Taobao.com
package com.taobao.tddl.interact.rule.ruleimpl;

import java.util.Map;

import com.taobao.tddl.interact.rule.virtualnode.VirtualNodeMap;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-8-8ÏÂÎç07:49:15
 */
public class DbVirtualNodeRule extends VirtualNodeGroovyRule {
	
	public DbVirtualNodeRule(String expression, VirtualNodeMap vNodeMap) {
		super(expression, vNodeMap);
	}
	
	public DbVirtualNodeRule(String expression, VirtualNodeMap vNodeMap,
			String extraPackagesStr) {
		super(expression, vNodeMap, extraPackagesStr);
	}

	@Override
	public String eval(Map<String, Object> columnValues, Object outerContext) {
		String key=(String) columnValues.get(EnumerativeRule.REAL_TABLE_NAME_KEY);
		return super.map(key);
	}
}
