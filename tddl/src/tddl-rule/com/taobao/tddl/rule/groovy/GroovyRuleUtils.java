package com.taobao.tddl.rule.groovy;


import java.util.Map;
import java.util.Map.Entry;

public class GroovyRuleUtils {
	public static final String RULE_CONTEXT ="context";
	public static final String IMPORT_STATIC_METHOD ="import static com.taobao.tddl.rule.groovy.staticmethod.GroovyStaticMethod.*;";
	
//	public static Script parseExpression(String expression){
//		Script script= null;
//		GroovyShell sh = new GroovyShell();
//		StringBuilder sb =new StringBuilder();
//		sb.append(IMPORT_STATIC_METHOD);
//		sb.append("");
//		sb.append(expression);
//		script = sh.parse(sb.toString());
//		return script;
//	}
//
//	@SuppressWarnings("unchecked")
//	public static Object evalueateSamplingFieldCommon(SamplingField samplingField
//			,Script script,String expression){
//		List<String> columns = samplingField.getColumns();
//		List<Object> fields = samplingField.getEnumFields();
//		int size = columns.size();
//		Binding bind = new Binding();
//		//设置当前的Context
//		for(int i = 0;i<size;i++){
//			bind.setVariable(columns.get(i),fields.get(i));
//		}
//		Object ret =null;
//		try {
//			script.setBinding(bind);
//			ret = script.run();
//		} catch (Throwable e) {
//			throw new RuntimeException("执行脚本出错，context:脚本:"+expression+"|参数"+buildArgumentsOutput(bind.getVariables()),e);
//		}
//		return ret;
//	}
	
	protected static String buildArgumentsOutput(Map<Object,Object> var){
		StringBuilder sb = new StringBuilder();
		if(var == null){
			return "do not have variable";
		}
		for(Entry<Object,Object> entry: var.entrySet()){
			sb.append("[").append(entry.getKey()).append("=").append(entry.getValue())
			.append("|type:").append(entry.getValue()==null?null:entry.getValue().getClass()).append("]");
		}
		return sb.toString();
	}
}
