package com.taobao.tddl.rule.ruleengine.rule;

/**
 * 因为计算以后会拿到结果，对于映射规则则可以多拿到一个mappingKey
 * 
 * @author shenxun
 *
 */
public class ResultAndMappingKey {
	public ResultAndMappingKey(String result) {
		this.result = result;
	}
	
	public final String result;

	Object mappingKey;

	String mappingTargetColumn;
	
}
