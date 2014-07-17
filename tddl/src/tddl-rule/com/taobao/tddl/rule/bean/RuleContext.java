//Copyright(c) Taobao.com
package com.taobao.tddl.rule.bean;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.interact.bean.Field;
import com.taobao.tddl.rule.LogicTableRule;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.RuleChain;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.CalculationContextInternal;

/**
 * @description 规则计算的内部context,主要放一些一次sql操作中原本需要多次重复计算但不会改变的结果,以及
 *              计算库和表时都需要的一些规则信息和共享信息.
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-12-03下午01:14:52
 */
public class RuleContext {
	/**
	 * 即配置中的TableRule
	 */
	protected LogicTableRule rule=null;
	
	/**
	 * 是否需要sourceKey
	 * 默认不需要,join和使用id in归组需要
	 */
	protected boolean needSourceKey=false;
	
	/**
	 * 表计算上下文
	 */
	protected Map<RuleChain, CalculationContextInternal/* 待计算的结果 */> calContextMap;

	/**
	 * 是否是第一次计算表
	 */
	protected boolean firstTableCalculate=true;
	
	/**
	 * 已经枚举过的表参数
	 */
	protected Map<String,Set<Object>> tabArgsMap;
	
	/**
	 * 表规则没有时的sourceKey Map
	 */
	protected Map<String, Field> tabSourceWithNoRule;
	
	/**
	 * 分库分表键交集
	 */
	protected List<String> dbAndTabWithSameColumn;

	/**
	 * 库表没有交集参数时,缓存表计算结果
	 */
	protected Map<String,Field> tabResultSet;
	
	public LogicTableRule getRule() {
		return rule;
	}

	public void setRule(LogicTableRule rule) {
		this.rule = rule;
	}

	public Map<RuleChain, CalculationContextInternal> getCalContextMap() {
		return calContextMap;
	}

	public void setCalContextMap(
			Map<RuleChain, CalculationContextInternal> calContextMap) {
		this.calContextMap = calContextMap;
	}

	public boolean isNeedSourceKey() {
		return needSourceKey;
	}

	public void setNeedSourceKey(boolean needSourceKey) {
		this.needSourceKey = needSourceKey;
	}
}
