package com.taobao.tddl.client.dispatcher;

import java.util.List;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.bean.MatcherResult;
import com.taobao.tddl.rule.LogicTableRule;

/**
 * 匹配对象用的借口，会将sql计算出的结果，参数以及规则进行匹配
 * @author shenxun
 * @author junyu
 */
public interface Matcher {
	/**
	 * 这里SqlParserResult pr + List<Object> args还需要抽象出一个更小的对象/接口
	 * 方便业务通过ThreadLocal方式绕过解析，直接指定
	 */
	MatcherResult match(ComparativeMapChoicer comparativeMapChoicer, List<Object> args, LogicTableRule rule) ;

	/**
	 * 指定先算库再算表或者算单个库后马上算表
	 * @param useNewTypeRuleCalculate
	 * @param comparativeMapChoicer
	 * @param args
	 * @param rule
	 * @return
	 */
	MatcherResult match(boolean useNewTypeRuleCalculate,ComparativeMapChoicer comparativeMapChoicer, List<Object> args, LogicTableRule rule);

	/**
	 * 指定先算库再算表或者算单个库后马上算表,并且指定是否需要提供结果值和参数的对应关系(sourceKey)
	 * @param useNewTypeRuleCalculate
	 * @param comparativeMapChoicer
	 * @param args
	 * @param rule
	 * @return
	 */
	MatcherResult match(boolean useNewTypeRuleCalculate,boolean needSourceKey,ComparativeMapChoicer comparativeMapChoicer, List<Object> args, LogicTableRule rule);
}
