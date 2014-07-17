//package com.taobao.tddl.rule.groovy;
//
//import static com.taobao.tddl.rule.groovy.GroovyRuleUtils.evalueateSamplingFieldCommon;
//import static com.taobao.tddl.rule.groovy.GroovyRuleUtils.parseExpression;
//import groovy.lang.Binding;
//import groovy.lang.Script;
//
//import com.taobao.tddl.rule.ruleengine.cartesianproductcalculator.SamplingField;
//import com.taobao.tddl.rule.ruleengine.rule.CartesianProductBasedListResultRule;
//import com.taobao.tddl.rule.ruleengine.rule.Context;
//
//public class ContextGroovyListRuleEngine extends CartesianProductBasedListResultRule{
//	Binding bind = new Binding();
//	Script script = null;
//	
//	Context context =null;
//	
//	@Override
//	public void parse() {
//		script = parseExpression(expression);
//	}
//	
//	public String evalueateSamplingField(SamplingField samplingField) {
//		bind.setVariable("context", context);
//		
//		Object value = evalueateSamplingFieldCommon(samplingField, script,expression);
//		String retString =null;
//		try {
//			retString = (String)value;
//		} catch (ClassCastException e) {
//			throw new ClassCastException("脚本的返回值必须为String类型的值");
//		}
//		return retString;
//	}
//
//	public Context getContext() {
//		return context;
//	}
//
//	public void setContext(Context context) {
//		this.context = context;
//	}
//	
//	
//}
