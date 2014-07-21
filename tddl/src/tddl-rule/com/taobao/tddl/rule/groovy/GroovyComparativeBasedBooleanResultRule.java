//package com.taobao.tddl.rule.groovy;
//
//import static com.taobao.tddl.rule.groovy.GroovyRuleUtils.parseExpression;
//import groovy.lang.Binding;
//import groovy.lang.Script;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import com.taobao.tddl.rule.ruleengine.cache.SharedValueElement;
//import com.taobao.tddl.rule.ruleengine.rule.BooleanAbstractResultRule;
//import com.taobao.tddl.rule.ruleengine.rule.ListAbstractResultRule;
//
//public class GroovyComparativeBasedBooleanResultRule extends BooleanAbstractResultRule{
//
//	@Override
//	public boolean eval(
//	Map<String, SharedValueElement> sharedValueElementMap) {
//		//设置当前的Context
//		for(Entry<String, SharedValueElement> sharedValueElement:sharedValueElementMap.entrySet()){
//			bind.setVariable(sharedValueElement.getKey(), sharedValueElement.getValue().comp);
//		}
//		return (Boolean)script.run();
//	}
//
//	Binding bind = new Binding();
//	Script script = null;
//
//	@Override
//	public void parse() {
//		script = parseExpression(expression, bind);
//	}
//
//}
