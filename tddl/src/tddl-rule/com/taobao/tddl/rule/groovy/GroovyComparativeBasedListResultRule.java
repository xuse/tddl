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
//import com.taobao.tddl.common.sqljep.function.Comparative;
//import com.taobao.tddl.rule.ruleengine.rule.ListAbstractResultRule;
//
//public class GroovyComparativeBasedListResultRule extends ListAbstractResultRule{
//
//	@Override
//	@SuppressWarnings("unchecked")
//	public List<String> eval(
//			Map<String, Comparative> sharedValueElementMap) {
//		
//		//设置当前的Context
//		for(Entry<String, Comparative> sharedValueElement:sharedValueElementMap.entrySet()){
//			bind.setVariable(sharedValueElement.getKey(), sharedValueElement.getValue());
//		}
//		return (List<String>)script.run();
//	}
//
//	Binding bind = new Binding();
//	Script script = null;
//
//	@Override
//	public void parse() {
//		script = parseExpression(expression);
//	}
//
//}
