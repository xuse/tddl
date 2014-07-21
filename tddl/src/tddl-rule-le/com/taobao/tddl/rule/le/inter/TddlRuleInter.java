//Copyright(c) Taobao.com
package com.taobao.tddl.rule.le.inter;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.rule.le.bean.TargetDatabase;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-5-4下午06:26:11
 */
public interface TddlRuleInter {
	/**
	 * single no versioned rule support,
	 * this rule can be <b>local rule</b>,
	 * <b>dynamic rule string</b>,or <b>remote diamond rule</b>
	 * 
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public List<TargetDatabase> route(String vtab,String conditionStr);
	
	/**
	 * multi versioned rule support,
	 * this rule only can be <b>remote versioned diamond rule</b>
	 * 
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public Map<String, List<TargetDatabase>> routeMultiVersion(String vtab,
			String conditionStr);
	
	/**
	 * multi versioned rule support,
	 * this rule only can be <b>remote versioned diamond rule</b>
	 * but the condition with the ComparativeMapChoicer
	 * 
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public Map<String, List<TargetDatabase>> routeMultiVersion(String vtab,
			ComparativeMapChoicer choicer);
	
	/**
	 * multi versioned rule support,specify the rule version
	 * this rule only can be <b>remote versioned diamond rule</b>
	 * 
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public List<TargetDatabase> routeWithSpecifyRuleVersion(String vtab,
			String conditionStr, String version);
}
