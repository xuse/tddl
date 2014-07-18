package com.taobao.tddl.interact.rule;

import java.util.Map;

public interface ShardingFunction {
	public Object eval(Map map, Object outerCtx);
}
