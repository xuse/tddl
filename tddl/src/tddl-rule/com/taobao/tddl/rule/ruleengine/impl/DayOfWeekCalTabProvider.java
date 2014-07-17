package com.taobao.tddl.rule.ruleengine.impl;

import java.util.Calendar;

public class DayOfWeekCalTabProvider extends CommonTableRuleProvider {

	@Override
	protected int getCalendarType() {
		return Calendar.DAY_OF_WEEK;
	}

}
