package com.taobao.tddl.sqlobjecttree;

import java.util.List;

import com.taobao.tddl.common.sqlobjecttree.Value;

public interface Function extends Value{
	public void setValue(List<Object> values);
	/**
	 * 获得在函数中的列名，如果有两个参数都为列名则抛出异常
	 * @return	the column name in function,
	 * 		 null if no nestedColName
	 */
	public String getNestedColName();
}
