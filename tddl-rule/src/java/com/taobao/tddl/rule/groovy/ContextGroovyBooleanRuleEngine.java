//package com.taobao.tddl.rule.groovy;
//
//import groovy.lang.Binding;
//import groovy.lang.Script;
//
//import com.taobao.tddl.rule.ruleengine.cartesianproductcalculator.SamplingField;
//import com.taobao.tddl.rule.ruleengine.rule.CartesianProductBasedBooleanResultRule;
//import com.taobao.tddl.rule.ruleengine.rule.Context;
//
//import static com.taobao.tddl.rule.groovy.GroovyRuleUtils.*;
//
//public class ContextGroovyBooleanRuleEngine extends CartesianProductBasedBooleanResultRule {
//	Context context =null;
//	
//	Binding bind = new Binding();
//	Script script = null;
//
//	
//	public Context getContext() {
//		return context;
//	}
//
//	public void setContext(Context context) {
//		this.context = context;
//	}
//
//	@Override
//	public void parse() {
//		
//		script = parseExpression(expression, bind);
//	}
//
//	public boolean evalueateSamplingField(SamplingField samplingField) {
//		bind.setVariable("context", context);
//		Object value = evalueateSamplingFieldCommon(samplingField, bind, script,expression);
//		Boolean bool = null;
//		if (value == null) {
//			return false;
//		}
//		try {
//			bool = (Boolean) value;
//		} catch (ClassCastException e) {
//			throw new ClassCastException("脚本的返回值必须为Boolean类型的值");
//		}
//		return bool;
//	}
//
//}
