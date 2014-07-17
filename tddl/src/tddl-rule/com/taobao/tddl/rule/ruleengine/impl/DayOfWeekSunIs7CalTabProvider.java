package com.taobao.tddl.rule.ruleengine.impl;

import java.util.Calendar;

public class DayOfWeekSunIs7CalTabProvider extends CommonTableRuleProvider {

	@Override
	protected int getCalendarType() {
		return Calendar.DAY_OF_WEEK;
	}

	@Override
	protected int getReturnInt(Calendar cal, int calType) {
		int ret = cal.get(calType)-1;
		if(ret == 0){
			ret = 7;
		}
		return ret;
	}
}
