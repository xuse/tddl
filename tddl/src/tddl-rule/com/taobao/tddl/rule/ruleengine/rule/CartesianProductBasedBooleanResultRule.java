//package com.taobao.tddl.rule.ruleengine.rule;
//
//import java.util.Map;
//import java.util.Set;
//
//import com.taobao.tddl.rule.ruleengine.cache.SharedValueElement;
//import com.taobao.tddl.rule.ruleengine.cartesianproductcalculator.CartesianProductCalculator;
//import com.taobao.tddl.rule.ruleengine.cartesianproductcalculator.SamplingField;
//
//
//public abstract class CartesianProductBasedBooleanResultRule extends BooleanAbstractResultRule{
//	
//	/**
//	 * 是否需要对交集内的数据取抽样点
//	 */
//	private boolean needMergeValueInCloseInterval = false;
//	
//	//TODO:boolean计算从原则上也应该分为两个步骤，第一个步骤是算函数，第二个部分是聚合运算
//	public boolean eval(Map<String,SharedValueElement> sharedValueElementMap){
//		
//		Map<String, Set<Object>> enumeratedMap = CartesianProductUtils.getSamplingField(sharedValueElementMap, needMergeValueInCloseInterval);
//		CartesianProductCalculator cartiesianProductCalculator = new CartesianProductCalculator(
//				enumeratedMap);
//	
//		for(SamplingField samplingField:cartiesianProductCalculator){
//			//枚举笛卡尔积里面的每一个值，进行运算，如果为true则直接返回
//			boolean isTrue = evalueateSamplingField(samplingField);
//			if(isTrue){
//				return true;
//			}
//		}
//		return false;
//	}
//	/**
//	 * 根据一组参数，计算出一个结果
//	 * @return
//	 */
//	public abstract boolean evalueateSamplingField(SamplingField samplingField);
//	public boolean isNeedMergeValueInCloseInterval() {
//		return needMergeValueInCloseInterval;
//	}
//	public void setNeedMergeValueInCloseInterval(
//			boolean needMergeValueInCloseInterval) {
//		this.needMergeValueInCloseInterval = needMergeValueInCloseInterval;
//	}
//	
//}
