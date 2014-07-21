package com.taobao.tddl.rule.ruleengine.impl;

import java.util.Calendar;

/**
 * month of year 处理器
 * @author shenxun
 *
 */
public class MonthCalTabProvider extends CommonTableRuleProvider{

	@Override
	protected int getCalendarType() {
		return Calendar.MONTH;
	}

}
