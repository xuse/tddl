package com.taobao.tddl.rule.groovy;

import groovy.lang.GroovyClassLoader;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.groovy.control.CompilationFailedException;

import com.taobao.tddl.interact.rule.bean.AdvancedParameter;
import com.taobao.tddl.interact.rule.bean.ExtraParameterContext;
import com.taobao.tddl.interact.rule.bean.SamplingField;
import com.taobao.tddl.rule.ruleengine.rule.CartesianProductBasedListResultRule;
import com.taobao.tddl.rule.ruleengine.rule.ResultAndMappingKey;

public class GroovyListRuleEngine extends CartesianProductBasedListResultRule {
	private static final Log logger = LogFactory
			.getLog(GroovyListRuleEngine.class);
	private Object ruleObj;
	private Method m_routingRuleMap;
	private static final String IMPORT_STATIC_METHOD = "import static com.taobao.tddl.rule.groovy.staticmethod.GroovyStaticMethod.*;";
	// 应用置入的上下文，可以用在evel的groovy脚本里
	private static final String IMPORT_EXTRA_PARAMETER_CONTEXT = "import com.taobao.tddl.interact.rule.bean.ExtraParameterContext;";

	private Map<String, Object> context;

	protected void initInternal() {
		if (expression == null) {
			throw new IllegalArgumentException("未指定 expression");
		}
		GroovyClassLoader loader = new GroovyClassLoader(
				GroovyListRuleEngine.class.getClassLoader());
		String groovyRule = getGroovyRule(expression,extraPackagesStr);
		Class<?> c_groovy;
		try {
			c_groovy = loader.parseClass(groovyRule);
		} catch (CompilationFailedException e) {
			throw new IllegalArgumentException(groovyRule, e);
		}

		try {
			// 新建类实例
			ruleObj = c_groovy.newInstance();
			// 获取方法
			m_routingRuleMap = getMethod(c_groovy, "eval", Map.class,
					ExtraParameterContext.class);
			if (m_routingRuleMap == null) {
				throw new IllegalArgumentException("规则方法没找到");
			}
			m_routingRuleMap.setAccessible(true);

		} catch (Throwable t) {
			throw new IllegalArgumentException("实例化规则对象失败", t);
		}
	}

	private static final Pattern RETURN_WHOLE_WORD_PATTERN = Pattern.compile(
			"\\breturn\\b", Pattern.CASE_INSENSITIVE);// 全字匹配
	private static final Pattern DOLLER_PATTERN = Pattern.compile("#.*?#");
    
	protected String getGroovyRule(String expression){
		return this.getGroovyRule(expression, null);
	}
	
	/**
	 * ex:Integer.valueOf(#userIdStr#.substring(0,1),16).intdiv(8)
	 */
	protected String getGroovyRule(String expression,String extraPackagesStr) {
		StringBuffer sb = new StringBuffer();
		//添加用户自定义package,已经处理
		if(extraPackagesStr!=null){
		    sb.append(extraPackagesStr);
		}
		sb.append(IMPORT_STATIC_METHOD);
		sb.append(IMPORT_EXTRA_PARAMETER_CONTEXT);
		Set<AdvancedParameter> params = new HashSet<AdvancedParameter>();
		Matcher matcher = DOLLER_PATTERN.matcher(expression);
		sb.append("public class RULE ").append("{");
		sb.append("public Object eval(Map map,ExtraParameterContext extraParameterContext){");

		// 替换并组装advancedParameter
		int start = 0;

		Matcher returnMarcher = RETURN_WHOLE_WORD_PATTERN.matcher(expression);
		if (!returnMarcher.find()) {
			sb.append("return ");
		}

		while (matcher.find(start)) {
			String realParam = matcher.group();
			realParam = realParam.substring(1, realParam.length() - 1);
			AdvancedParameter advancedParameter = AdvancedParameter.getAdvancedParamByParamTokenNew(realParam,false);
			//modify by junyu
			if (isNeedAdd(params, advancedParameter)) {
				params.add(advancedParameter);
			}
			sb.append(expression.substring(start, matcher.start()));
			sb.append("(map.get(\"");
			// 替换成(map.get("key"));
			sb.append(advancedParameter.key);
			sb.append("\"))");

			start = matcher.end();
		}
		// 设置需要用到的参数
		setAdvancedParameter(params);
		sb.append(expression.substring(start));
		sb.append(";");
		sb.append("};");
		sb.append("}");
		logger.debug(sb.toString());
		return sb.toString();
	}

	public ResultAndMappingKey evalueateSamplingField(
			SamplingField samplingField) {
		List<String> columns = samplingField.getColumns();
		List<Object> values = samplingField.getEnumFields();

		int size = columns.size();
		Map<String, Object> argumentMap = new HashMap<String, Object>(size);
		for (int i = 0; i < size; i++) {
			argumentMap.put(columns.get(i), values.get(i));
		}
		// 放入应用自定义字段
		if (this.context != null) {
			for (Map.Entry<String, Object> entry : this.context.entrySet()) {
				argumentMap.put(entry.getKey(), entry.getValue());
			}
		}

		Object[] args = new Object[] { argumentMap };
		String result = imvokeMethod(args);
		if (result != null) {
			return new ResultAndMappingKey(result);
		} else {
			throw new IllegalArgumentException("规则引擎的结果不能为null");
		}
	}

	public ResultAndMappingKey evalueateSamplingField(
			SamplingField samplingField,
			ExtraParameterContext extraParameterContext) {
		List<String> columns = samplingField.getColumns();
		List<Object> values = samplingField.getEnumFields();

		int size = columns.size();
		Map<String, Object> argumentMap = new HashMap<String, Object>(size);
		for (int i = 0; i < size; i++) {
			argumentMap.put(columns.get(i), values.get(i));
		}
		// 放入应用自定义字段
		if (this.context != null) {
			for (Map.Entry<String, Object> entry : this.context.entrySet()) {
				argumentMap.put(entry.getKey(), entry.getValue());
			}
		}

		Object[] args = new Object[] { argumentMap, extraParameterContext };
		String result = imvokeMethod(args);
		if (result != null) {
			return new ResultAndMappingKey(result);
		} else {
			throw new IllegalArgumentException("规则引擎的结果不能为null");
		}
	}
	
	/**
	 * 针对单column和单个value
	 * @param column
	 * @param value
	 * @param extraParameterContext
	 * @return
	 */
	public ResultAndMappingKey evalueateSimpleColumAndValue(
			String column,Object value,
			ExtraParameterContext extraParameterContext) {
		Map<String, Object> argumentMap = new HashMap<String, Object>(1);
		argumentMap.put(column, value);
		// 放入应用自定义字段
		if (this.context != null) {
			for (Map.Entry<String, Object> entry : this.context.entrySet()) {
				argumentMap.put(entry.getKey(), entry.getValue());
			}
		}

		Object[] args = new Object[] { argumentMap, extraParameterContext };
		String result = imvokeMethod(args);
		if (result != null) {
			return new ResultAndMappingKey(result);
		} else {
			throw new IllegalArgumentException("规则引擎的结果不能为null");
		}
	}

	/**
	 * 调用目标方法
	 * 
	 * @param args
	 * @return
	 */
	public String imvokeMethod(Object[] args) {
		Object value = invoke(ruleObj, m_routingRuleMap, args);
		String retString = null;
		if (value == null) {
			return null;
		} else {
			retString = String.valueOf(value);
			return retString;
		}
	}

	private static Method getMethod(Class<?> c, String name,
			Class<?>... parameterTypes) {
		try {
			return c.getMethod(name, parameterTypes);
		} catch (SecurityException e) {
			throw new IllegalArgumentException("实例化规则对象失败", e);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException("没有这个方法" + name, e);
		}
	}

	private static Object invoke(Object obj, Method m, Object... args) {
		try {
			return m.invoke(obj, args);
		} catch (Throwable t) {
			// logger.warn("调用方法：" + m + "失败", t);
			// return null;
			throw new IllegalArgumentException("调用方法失败: " + m + t.getCause(), t);
		}
	}

	/**
	 * 如果一条规则里面有重复出现parameter,那么以第一个为准
	 * 此处没有进行paramSet是否为null的检查，由外部保障。
	 * 
	 * @param paramSet
	 * @param param
	 * @return
	 */
	private boolean isNeedAdd(Set<AdvancedParameter> paramSet,
			AdvancedParameter param) {
		for (AdvancedParameter ap : paramSet) {
			if (param.key.equals(ap.key)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String toString() {
		return "GroovyListRuleEngine [expression=" + expression
				+ ", parameters=" + parameters + "]";
	}

}
