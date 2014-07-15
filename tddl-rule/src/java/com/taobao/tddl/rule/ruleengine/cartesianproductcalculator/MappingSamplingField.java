package com.taobao.tddl.rule.ruleengine.cartesianproductcalculator;

import java.util.List;

import com.taobao.tddl.interact.rule.bean.SamplingField;

/**
 * 添加了映射附带的字段，
 * 
 * @author shenxun
 *
 */
public abstract class MappingSamplingField extends SamplingField{

	public MappingSamplingField(List<String> columns,int capacity) {
		super(columns,capacity);
	}

}
