package com.taobao.tddl.sqlobjecttree;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface OutputHandler {
	public String handle(Map<String,String> table,List<Object> param,List<Object> modifiableTableName,Set<String> originalTable,Number skip,Number max,Map<Integer, Object> changeParam);
}
