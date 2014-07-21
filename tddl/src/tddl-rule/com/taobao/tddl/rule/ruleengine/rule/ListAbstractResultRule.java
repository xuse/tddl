package com.taobao.tddl.rule.ruleengine.rule;

import java.util.Map;
import java.util.Set;

import com.taobao.tddl.interact.bean.Field;
import com.taobao.tddl.interact.rule.bean.ExtraParameterContext;
import com.taobao.tddl.interact.rule.bean.SamplingField;
import com.taobao.tddl.interact.sqljep.Comparative;

public abstract class ListAbstractResultRule extends AbstractRule {
	/**
	 * 此方法已经未使用，替换的方法下面，为了单元测试兼容性
	 * 用作分库
	 * 
	 * 
	 * @param sharedValueElementMap
	 * @return 返回的map不会为null,但有可能为空的map，如果map不为空，则内部的子map必定不为空。最少会有一个值
	 */
/*	public abstract Map<String column , Field> eval(
			Map<String, Comparative> sharedValueElementMap);
*/
	/**
	 * 此方法已经未使用，替换的方法下面，为了单元测试兼容性
	 * 
	 * 用作分表。不带有对计算出当前值的函数的源的追踪信息
	 * 
	 * @param enumeratedMap
	 *            列名->枚举 对应表
	 * @param mappingTargetColumn
	 *            映射规则列
	 * @param mappingKeys
	 *            映射规则值
	 * 
	 * @return 结果集字段，不会为空 如果子类方法设置了在set为空时抛异常，则会自动抛出
	 */
/*	public abstract Map<String 结果的值 , Field> evalElement(
			Map<String, Set<Object>> enumeratedMap);*/

	// public abstract Set<String> evalWithoutSourceTrace(Map<String,
	// Set<Object>> enumeratedMap);

	public abstract Map<String/* column */, Field> eval(
			Map<String, Comparative> sharedValueElementMap,
			ExtraParameterContext extraParameterContext);

	public abstract Map<String/* 结果的值 */, Field> evalElement(
			Map<String, Set<Object>> enumeratedMap,
			ExtraParameterContext extraParameterContext);
	
	/**
	 * 得到描点参数值
	 * @param argumentsMap
	 * @return
	 */
	public abstract Map<String, Set<Object>> prepareEnumeratedMap(
			Map<String, Comparative> argumentsMap);
	
	/**
	 * 针对多column,多value的规则计算
	 * @param samplingField
	 * @param extraParameterContext
	 * @return
	 */
	public abstract ResultAndMappingKey evalueateSamplingField(
			SamplingField samplingField,ExtraParameterContext extraParameterContext);
	
	/**
	 * 针对单column,单value的规则计算
	 * @param column
	 * @param value
	 * @param extraParameterContext
	 * @return
	 */
	public abstract ResultAndMappingKey evalueateSimpleColumAndValue(
			String column,Object value,
			ExtraParameterContext extraParameterContext);
}
