package com.taobao.tddl.rule.ruleengine.rule;

import java.util.Map;

public interface Transmitable {
	void transmit(Map<String, Object> transmitted);
}
