package com.taobao.tddl.sqlobjecttree;

import java.util.Map;

/**
 * °ü×°Àà
 * 
 * @author whisper
 *
 */
public interface PageWrapper {
	void modifyParam(Number limitFrom, Number limitTo,Map<Integer, Object> changeParam);
	String getSqlReturn(Number limitFrom,Number limitTo); 
	Integer getIndex();
	boolean canBeChange();
	Number getValue();
}
