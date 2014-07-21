package com.taobao.tddl.client.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.client.dispatcher.Matcher;
import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.bean.MatcherResult;
import com.taobao.tddl.interact.bean.MatcherResultImp;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.LogicTableRule;
import com.taobao.tddl.rule.bean.RuleContext;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.RuleChain;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.CalculationContextInternal;

public class SpringBasedRuleMatcherImpl implements Matcher {
    public MatcherResult match(ComparativeMapChoicer comparativeMapChoicer, List<Object> args, LogicTableRule rule){
		return this.match(false, comparativeMapChoicer, args, rule);
	}
	
    public MatcherResult match(boolean useNewTypeRuleCalculate,ComparativeMapChoicer comparativeMapChoicer, List<Object> args, LogicTableRule rule){
    	return this.match(false,false,comparativeMapChoicer, args, rule);
    }
    
	public MatcherResult match(boolean useNewTypeRuleCalculate,boolean needSourceKey,ComparativeMapChoicer comparativeMapChoicer,
			List<Object> args, LogicTableRule rule) {
		// 规则链儿集合，包含了规则中所有规则链
		Set<RuleChain> ruleChainSet = rule.getRuleChainSet();
		// 符合要求的数据库分库字段和对应的值，如果有多个那么都会放在一起
		Map<String, Comparative> comparativeMapDatabase = new HashMap<String, Comparative>(
				2);
		// 符合要求的talbe分表字段和对应的值，如果有多个那么都会放在一起
		Map<String, Comparative> comparativeTable = new HashMap<String, Comparative>(
				2);

		Map<RuleChain, CalculationContextInternal/* 待计算的结果 */> resultMap = new HashMap<RuleChain, CalculationContextInternal>(
				ruleChainSet.size());

		for (RuleChain ruleChain : ruleChainSet) {

			// 针对每一个规则链
			List<Set<String>/* 每一条规则需要的参数 */> requiredArgumentSortByLevel = ruleChain
					.getRequiredArgumentSortByLevel();
			/*
			 * 因为ruleChain本身的个数是一定的，个数与getRequiredArgumentSortByLevel
			 * list的size一样多，因此不会越界
			 */
			int index = 0;

			for (Set<String> oneLevelArgument : requiredArgumentSortByLevel) {
				// 针对每一个规则链中的一个级别，级别是从低到高的首先查看是否满足规则要求，如果满足则进行运算
				Map<String/* 当前参数要求的列名 */, Comparative> sqlArgs = comparativeMapChoicer
						.getColumnsMap(args, oneLevelArgument);
				if (sqlArgs.size() == oneLevelArgument.size()) {
					// 表示匹配,规则链作为key,value为结果
					resultMap.put(ruleChain, new CalculationContextInternal(
							ruleChain, index, sqlArgs));
					if (ruleChain.isDatabaseRuleChain()) {
						comparativeMapDatabase.putAll(sqlArgs);
					} else {
						// isTableRuleChain
						comparativeTable.putAll(sqlArgs);
					}
					break;
				} else {
					index++;
				}
			}
		}
		
		RuleContext innerContext=new RuleContext();
		innerContext.setCalContextMap(resultMap);
		innerContext.setRule(rule);
		innerContext.setNeedSourceKey(needSourceKey);
		
		List<TargetDB> calc = useOneTypeCalculate(useNewTypeRuleCalculate,innerContext);
		return new MatcherResultImp(calc, comparativeMapDatabase,
				comparativeTable);
	}

    /**
     * 有2种计算策略可以选,第一种是先计算完库在计算表,第二种是一个库计算完毕马上进行表计算
     * @param firstDb      如果为true,选择先把库计算完毕,再计算表
     * @param ruleContext  规则计算内部context.
     * @return
     */
	private List<TargetDB> useOneTypeCalculate(boolean useNewTypeRuleCalculate,RuleContext ruleContext){
		if(!useNewTypeRuleCalculate){
		    return ruleContext.getRule().calculate(ruleContext.getCalContextMap());
		}else{
	        return ruleContext.getRule().calculateNew(ruleContext);	
		}
	}
}
