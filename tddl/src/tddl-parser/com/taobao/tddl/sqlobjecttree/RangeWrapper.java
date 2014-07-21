package com.taobao.tddl.sqlobjecttree;

import java.util.Map;

public class RangeWrapper extends PageWrapperCommon {

	public RangeWrapper(Object val) {
		super(val);
	}

	public String getSqlReturn(Number skip, Number max) {
		if (skip instanceof Long || max instanceof Long) {
			return String.valueOf(getSubLong(skip, max));
		} else if (skip instanceof Integer && max instanceof Integer) {
			return String.valueOf(getSubInt(skip, max));
		} else {
			throw new IllegalArgumentException("只支持int long的情况");
		}

	}

	private long getSubLong(Number skip, Number max) {
		return (max.longValue() - skip.longValue());
	}

	private int getSubInt(Number skip, Number max) {
		return (max.intValue() - skip.intValue());
	}

	public void modifyParam(Number skip, Number max, 
			Map<Integer, Object> modifiedMap) {
		if (skip instanceof Long || max instanceof Long) {
			modifiedMap.put(index, getSubLong(skip, max));
		} else if (skip instanceof Integer && max instanceof Integer) {
			modifiedMap.put(index, getSubInt(skip, max));
		} else {
			throw new IllegalArgumentException("只支持int long的情况");
		}
	}
}
