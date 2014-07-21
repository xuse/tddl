package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;

import java.util.Map;

import com.taobao.tddl.sqlobjecttree.PageWrapper;

public class NormalRangePlaceHandler extends RangePlaceHandler {
	
	public NormalRangePlaceHandler(Number skip, Number max) {
		super(skip, max);
	}
	public NormalRangePlaceHandler() {
		super();
	}

	@Override
	public String changeValue(PageWrapper pageWrapper,
			Map<Integer, Object> changeParam) {
		Integer index = pageWrapper.getIndex();
		Number value = pageWrapper.getValue();
		if (index != null) {
			return "?";
		} else if (value != null) {
			return value.toString();
		} else {
			throw new IllegalStateException("不应该出现没有值直接写在sql,但也没有index的情况");
		}
	}
}
