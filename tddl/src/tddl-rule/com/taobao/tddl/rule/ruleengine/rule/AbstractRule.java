package com.taobao.tddl.rule.ruleengine.rule;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.interact.rule.bean.AdvancedParameter;

/**
 * 规则的总抽象 规则由参数和表达式组成
 * 
 * @author shenxun
 * @author junyu
 */
public abstract class AbstractRule {
	Log log = LogFactory.getLog(AbstractRule.class);
	/**
	 * 当前规则需要用到的参数
	 */
	protected Set<AdvancedParameter> parameters;

	private boolean inited = false;

	/**
	 * 当前规则需要用到的表达式
	 */
	protected String expression;
	
	/**
	 * 用户自定义jar包package
	 */
	protected String extraPackagesStr;

	/*
	 * 通过分析库表结构规则智能计算叠加次数，并set到规则中，需要注意的是 如果有多个值参与了一个计算式，这种分析是不准确的，这时候可以通过配置文件
	 * 手动的优先设置针对每一个参与运算的参数的叠加次数。
	 * 
	 * 现在还没启用，因为比较复杂
	 * 
	 * @param cumulativeTimes
	 * 
	 * public void setCumulativeTimes(int cumulativeTimes){
	 * for(KeyAndAtomIncValue keyAndAtomIncValue :parameters){
	 * if(keyAndAtomIncValue.cumulativeTimes == null){
	 * keyAndAtomIncValue.cumulativeTimes = cumulativeTimes; } } }
	 */
	protected abstract void initInternal();

	/**
	 * 确保规则只初始化一次
	 */
	public void initRule() {
		if (inited) {
			log.debug("rule has inited");
		} else {
			initInternal();
			inited = true;
		}
	}

	/**
	 * spring注入带有默认自增字段的值,会将所有值变为小写
	 * 
	 * @param parameters
	 */
	public void setParameters(Set<String> parameters) {
		if (this.parameters == null) {
			this.parameters = new HashSet<AdvancedParameter>();
		}
		for (String str : parameters) {
			AdvancedParameter advancedParameter = AdvancedParameter.getAdvancedParamByParamTokenNew(str,false);
			this.parameters.add(advancedParameter);
		}
	}

	/**
	 * Spring注入多个
	 * 
	 * @param parameters
	 */
	public void setAdvancedParameter(Set<AdvancedParameter> parameters) {
		if (this.parameters == null) {
			this.parameters = new HashSet<AdvancedParameter>();
		}
		for (AdvancedParameter keyAndAtomIncValue : parameters) {
			this.parameters.add(keyAndAtomIncValue);
		}
	}

	/**
	 * spring注入一个
	 * 
	 * @param parameter
	 */
	public void setAdvancedParameter(AdvancedParameter parameter) {
		if (this.parameters == null) {
			this.parameters = new HashSet<AdvancedParameter>();
		}
		this.parameters.add(parameter);
	}
	
	public Set<AdvancedParameter> getParameters() {
		return parameters;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		if (expression != null)
			this.expression = expression;
	}

	public void setExtraPackagesStr(String extraPackagesStr) {
		this.extraPackagesStr = extraPackagesStr;
	}

	/**
	 * col,1,7|col1,1,7....
	 * 
	 * @param parameterArray
	 */
	public void setParameter(String parameterArray) {
		if (parameterArray != null && parameterArray.length() != 0) {
			String[] paramArray = parameterArray.split("\\|");
			Set<String> paramSet = new HashSet<String>(Arrays
					.asList(paramArray));
			this.setParameters(paramSet);
		}
	}
}
