package com.taobao.tddl.sqlobjecttree;

/**
 * select group_function from table where xxx
 * @author shenxun
 *
 */
public enum GroupFunctionType {
	MIN,MAX,COUNT,AVG,SUM,
	/**
	 * 非以上的任何一种情况 
	 */
	NORMAL
}
